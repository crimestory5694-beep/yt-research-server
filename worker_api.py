"""/worker/* endpoints: the ONLY surface the local worker talks to. Authentication (separate WORKER_AUTH_TOKEN(S)),
body-size limit and rate limit are enforced by security.SecurityMiddleware before these handlers run; this module
adds strict input validation and the "never acknowledge before persisted" ordering.

The worker never receives Upstash credentials: Render holds them and brokers every queue/cache operation.
"""
from __future__ import annotations

import re
from typing import Callable, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from job_queue import ERROR_CLASS_RE, JOB_ID_RE, LANG_RE, VIDEO_RE, QueueUnavailable, kind_for, split_job_id

WORKER_ID_PATTERN = r"^[A-Za-z0-9._-]{3,64}$"
LANG_PATTERN = r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})?$"
JOB_PATTERN = r"^[A-Za-z0-9_-]{11}:(any|[a-z]{2,3}(-[a-z0-9]{2,8})?)$"


class ClaimIn(BaseModel):
    worker_id: str = Field(pattern=WORKER_ID_PATTERN)
    max_jobs: int = Field(1, ge=1, le=5)
    lease_seconds: int = Field(300, ge=60, le=900)


class RenewIn(BaseModel):
    worker_id: str = Field(pattern=WORKER_ID_PATTERN)
    job_id: str = Field(pattern=JOB_PATTERN)
    lease_seconds: int = Field(300, ge=60, le=900)


class SegmentIn(BaseModel):
    text: str = Field(max_length=2000)
    start: float = Field(ge=0, le=1_000_000)
    duration: float = Field(0, ge=0, le=100_000)


class LangInfo(BaseModel):
    code: str = Field(max_length=16)
    name: str = Field("", max_length=80)
    auto_generated: bool = False


class ResultIn(BaseModel):
    language_code: str = Field(pattern=LANG_PATTERN)
    caption_type: Literal["manual", "auto_generated", "unknown"] = "unknown"
    available_languages: List[LangInfo] = Field(default_factory=list, max_length=200)
    segments: List[SegmentIn] = Field(default_factory=list, max_length=5000)
    total_segments: int = Field(0, ge=0, le=1_000_000)
    full_transcript: str = Field(min_length=20, max_length=3_000_000)
    source: Literal["youtube-transcript-api", "yt-dlp"]


class CompleteIn(BaseModel):
    worker_id: str = Field(pattern=WORKER_ID_PATTERN)
    job_id: str = Field(pattern=JOB_PATTERN)
    result: ResultIn


class FailIn(BaseModel):
    worker_id: str = Field(pattern=WORKER_ID_PATTERN)
    job_id: str = Field(pattern=JOB_PATTERN)
    error_class: str = Field(pattern=r"^[a-z][a-z_:]{2,40}$")
    message: str = Field("", max_length=200)

    @field_validator("message")
    @classmethod
    def _no_control(cls, v):
        return re.sub(r"[\x00-\x1f|]", " ", v)


def _err(status: int, code: str, **extra) -> JSONResponse:
    return JSONResponse({"error": code, **extra}, status_code=status)


def build_router(get_service: Callable) -> APIRouter:
    r = APIRouter(prefix="/worker", tags=["worker"])

    def queue_or_503():
        svc = get_service()
        if svc.queue is None:
            raise HTTPException(503, detail="queue_disabled")
        return svc

    @r.get("/ping")
    async def ping():
        svc = get_service()
        return {"ok": True, "queue_enabled": svc.queue is not None, "server_time": int(svc._clock())}

    @r.post("/claim")
    async def claim(body: ClaimIn):
        svc = queue_or_503()
        try:
            jobs = await svc.queue.claim(body.worker_id, body.max_jobs, body.lease_seconds)
            await svc.queue.maybe_prune()
        except QueueUnavailable:
            return _err(503, "queue_unavailable")
        return {"jobs": jobs, "server_time": int(svc._clock())}

    @r.post("/renew")
    async def renew(body: RenewIn):
        svc = queue_or_503()
        try:
            return await svc.queue.renew(body.job_id, body.worker_id, body.lease_seconds)
        except QueueUnavailable:
            return _err(503, "queue_unavailable")

    @r.post("/complete")
    async def complete(body: CompleteIn):
        svc = queue_or_503()
        vid, lang = split_job_id(body.job_id)
        try:
            rec = (await svc.queue.get_many([body.job_id])).get(body.job_id)
        except QueueUnavailable:
            return _err(503, "queue_unavailable")
        if rec is None:
            return _err(404, "unknown_job")
        res = body.result.model_dump()
        res["segments"] = res["segments"][: svc.cfg.max_segments_returned]
        res["total_segments"] = max(body.result.total_segments, len(body.result.segments))
        # 1) persist the transcript in the shared store FIRST; 2) only then mark the job completed.
        if not await svc.store_external_result(vid, lang, res, body.result.source):
            return _err(503, "cache_persist_failed")   # job stays leased/pending -> retried; nothing acknowledged
        try:
            state = await svc.queue.complete(body.job_id, body.worker_id)
        except QueueUnavailable:
            return _err(503, "queue_unavailable")      # transcript is cached; worker will retry the (idempotent) call
        return {"state": state}

    @r.post("/fail")
    async def fail(body: FailIn):
        svc = queue_or_503()
        vid, lang = split_job_id(body.job_id)
        if kind_for(body.error_class) == "definitive":
            await svc.store_external_negative(vid, lang, body.error_class, body.message or body.error_class)
        try:
            return await svc.queue.fail(body.job_id, body.worker_id, body.error_class, body.message)
        except QueueUnavailable:
            return _err(503, "queue_unavailable")

    @r.get("/jobs/{job_id}")
    async def job(job_id: str):
        if not JOB_ID_RE.match(job_id):
            raise HTTPException(422, detail="invalid_job_id")
        svc = queue_or_503()
        try:
            rec = (await svc.queue.get_many([job_id])).get(job_id)
        except QueueUnavailable:
            return _err(503, "queue_unavailable")
        if rec is None:
            return _err(404, "unknown_job")
        return {"job_id": job_id, **rec}

    @r.get("/result/{video_id}")
    async def result(video_id: str, language: str = "en"):
        if not VIDEO_RE.match(video_id) or not LANG_RE.match(language.lower()):
            raise HTTPException(422, detail="invalid_parameters")
        svc = queue_or_503()
        res = await svc.cached_result(video_id, language.lower())
        if res is None:
            return _err(404, "not_cached")
        return res

    return r
