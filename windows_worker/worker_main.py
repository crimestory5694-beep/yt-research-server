"""Entry point of the local transcript worker.

    python worker_main.py --run                              long-running loop (what Task Scheduler starts)
    python worker_main.py --once                             one poll cycle, then exit
    python worker_main.py --self-test                        prints what the connectivity test would do (no network)
    python worker_main.py --self-test --i-approve-live-test --ids ID1 ID2 ID3     the live test (see README)
    python worker_main.py --status                           local pacing state, no network
"""
import argparse
import atexit
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))          # repo root: transcript_service.py, ytdlp_provider.py, transcript_probe.py
sys.path.insert(0, HERE)

import worker_core as wc  # noqa: E402


def app_dir() -> str:
    base = os.environ.get("APPDATA") or os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, "yt-worker")


def load_env_file(path: str) -> dict:
    env = {}
    try:
        with open(path, encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return env


def load_settings() -> wc.Settings:
    d = os.environ.get("YT_WORKER_HOME", app_dir())
    env = {**load_env_file(os.path.join(d, "worker.env")), **{k: v for k, v in os.environ.items() if k in (
        "RENDER_URL", "WORKER_TOKEN", "WORKER_ID")}}
    env.setdefault("STATE_DIR", d)
    return wc.Settings.from_env(env)


def setup_logging(s: wc.Settings):
    os.makedirs(s.state_dir, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(os.path.join(s.state_dir, "worker.log"), maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    h.addFilter(wc.RedactingFilter([s.token, s.doorbell_token]))
    root = logging.getLogger("yt")
    root.setLevel(logging.INFO)
    root.addHandler(h)
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)   # their URLs/headers are not needed in logs


class SingleInstance:
    """Only one worker per user: a second Task Scheduler launch exits quietly."""

    def __init__(self, directory):
        os.makedirs(directory, exist_ok=True)
        self.f = open(os.path.join(directory, "worker.lock"), "a+")
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.ok = True
        except OSError:
            self.ok = False


def build_worker(s: wc.Settings, stop: threading.Event) -> wc.Worker:
    state = wc.State(s.state_dir)
    return wc.Worker(s, wc.Api(s), state, wc.default_extractor(s.enable_ytdlp), stop=stop, doorbell=wc.make_doorbell(s))


def self_test(s: wc.Settings, ids: list, approved: bool) -> int:
    plan = ("CONNECTIVITY TEST (does not claim or change any job):\n"
            "  1. one authenticated HTTPS call to <RENDER_URL>/worker/ping\n"
            "  2. one plain HTTPS GET of https://www.youtube.com/ (reachability)\n"
            f"  3. up to 3 transcript lookups ({', '.join(ids[:3]) or 'no ids given'}), >=10 s apart, via youtube-transcript-api\n"
            "Prints only statuses - no transcript text, no secrets. Needs the owner's approval: re-run with --i-approve-live-test.")
    if not approved:
        print(plan); return 0
    import transcript_probe as tp
    import transcript_service as ts
    api = wc.Api(s)
    try:
        info = api.ping(); print("render:", {"ok": info.get("ok"), "queue_enabled": info.get("queue_enabled")})
    except wc.AuthError:
        print("render: TOKEN REJECTED (check WORKER_TOKEN)"); return 3
    except wc.ServerBusy as e:
        print("render: unreachable", e); return 2
    reach = tp.reachability(); print("youtube reachability:", reach)
    results = []
    for i, vid in enumerate(ids[:3]):
        if i:
            time.sleep(10)
        t0 = time.time()
        try:
            ts.fetch_with_library(vid, ["en"], True, None, 20); st = "ok"
        except ts.TranscriptError as e:
            st = e.status
        results.append({"video_id": vid, "label": "user-supplied", "status": st, "seconds": round(time.time() - t0, 1)})
        print(" ", vid, st)
    v = tp.decide(reach, results); print("verdict:", v); return 0 if v["verdict"] == "FREE_EXTRACTION_WORKS" else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--run", action="store_true"); g.add_argument("--once", action="store_true")
    g.add_argument("--self-test", action="store_true"); g.add_argument("--status", action="store_true")
    ap.add_argument("--i-approve-live-test", action="store_true"); ap.add_argument("--ids", nargs="*", default=[])
    a = ap.parse_args(argv)
    try:
        s = load_settings()
    except ValueError as e:
        print("Configuration problem:", e, file=sys.stderr); return 2
    if a.self_test:
        return self_test(s, a.ids, a.i_approve_live_test)
    if a.status:
        st = wc.State(s.state_dir); h, d = st.counts()
        print({"extractions_last_hour": h, "extractions_last_24h": d, "paused_for_seconds": int(st.paused_for()),
               "blocks_in_row": st.data["blocks_in_row"], "waiting_in_outbox": len(os.listdir(os.path.join(s.state_dir, "outbox")))}); return 0
    setup_logging(s)
    lock = SingleInstance(s.state_dir)
    if not lock.ok:
        logging.getLogger("yt.worker").info("another worker instance is running; exiting"); return 0
    stop = threading.Event()
    for sig in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, sig):
            signal.signal(getattr(signal, sig), lambda *_: stop.set())
    w = build_worker(s, stop)
    if a.once:
        print(w.run_cycle()); return 0
    return w.run_forever()


if __name__ == "__main__":
    sys.exit(main())
