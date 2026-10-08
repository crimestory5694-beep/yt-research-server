"""Offline tests of the probe's decision logic and safety properties (no network)."""

import transcript_probe as lp
import transcript_service as ts
from tests.helpers import E, FakeLibrary, good_data, make_service

R = lambda *s: [{"label": "x", "status": v} for v in s] + [{"label": "control-nonexistent", "status": "video_unavailable"}]
OKR = {"ok": True, "captcha_page": False}


def test_decide_matrix():
    assert lp.decide(OKR, R("ok", "blocked"))["verdict"] == "FREE_EXTRACTION_WORKS"
    assert lp.decide({"ok": False}, R("network_error"))["verdict"] == "NO_NETWORK_PATH_TO_YOUTUBE"
    assert lp.decide(OKR, R("blocked", "blocked"))["verdict"] == "YOUTUBE_IP_BLOCKED"
    assert lp.decide({"ok": True, "captcha_page": True}, R("parse_error"))["verdict"] == "YOUTUBE_IP_BLOCKED"
    assert lp.decide(OKR, R("blocked", "proxy_error"))["verdict"] == "PROXY_CONFIG_FAILURE"
    assert lp.decide(OKR, R("no_captions", "language_unavailable"))["verdict"].startswith("INCONCLUSIVE")
    assert lp.decide(OKR, R("parse_error", "timeout"))["verdict"] == "PROVIDER_OR_PARSER_ERROR"


async def test_probe_never_calls_paid_even_with_keys(monkeypatch):
    for k, v in {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "TRANSCRIPT_API_KEY": "k1", "SUPADATA_API_KEY": "k2"}.items():
        monkeypatch.setenv(k, v)
    calls = []
    class Boom:
        def __call__(self): calls.append(1); raise AssertionError("paid http client must not be created")
    svc = make_service({"ENABLE_PAID_TRANSCRIPT_APIS": "true", "TRANSCRIPT_API_KEY": "k1"}, lib=FakeLibrary(E(ts.BLOCKED)))
    svc._http_factory = Boom()
    svc.cfg.enable_paid = False
    out = await lp.run(service=svc, delay=0, reach={"ok": True, "captcha_page": False})
    assert calls == [] and out["verdict"] == "YOUTUBE_IP_BLOCKED" and out["paid_apis"] == "disabled"
    assert "k1" not in str(out) and "full_transcript" not in str(out)


async def test_probe_reports_success_without_printing_text():
    out = await lp.run(service=make_service(lib=FakeLibrary(good_data())), delay=0, reach={"ok": True, "captcha_page": False})
    assert out["verdict"] == "FREE_EXTRACTION_WORKS" and "word0" not in str(out)


def test_startup_probe_off_by_default_and_on_when_enabled(monkeypatch, capsys):
    import asyncio
    import main
    from fastapi.testclient import TestClient
    started = []

    async def fake(): started.append(1)
    monkeypatch.setattr(lp, "run_and_log", fake)
    monkeypatch.delenv("RUN_TRANSCRIPT_PROBE_ON_STARTUP", raising=False)
    with TestClient(main.app):
        pass
    assert started == []
    monkeypatch.setenv("RUN_TRANSCRIPT_PROBE_ON_STARTUP", "true")
    with TestClient(main.app):
        pass
    assert started == [1]


async def test_run_and_log_prints_single_parseable_line_and_never_raises(monkeypatch, capsys):
    import json

    async def boom(*a, **k): raise RuntimeError("secret-ish detail")
    monkeypatch.setattr(lp, "run", boom)
    out = await lp.run_and_log()
    line = capsys.readouterr().out.strip()
    assert line.startswith("TRANSCRIPT_PROBE_RESULT ") and out["verdict"] == "PROBE_CRASHED"
    assert json.loads(line.split(" ", 1)[1])["verdict"] == "PROBE_CRASHED" and "secret-ish" not in line


def test_cli_wrapper_runs_from_any_cwd(tmp_path):
    import subprocess, sys, os
    script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "live_probe.py")
    r = subprocess.run([sys.executable, "-c", "import runpy,sys; sys.argv=['x']; runpy.run_path(%r, run_name='notmain')" % script],
                       cwd=tmp_path, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr      # imports resolve outside the repo root (earlier version failed here)
