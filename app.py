# app.py
import os
import subprocess
import threading
from fastapi import FastAPI, Header, HTTPException
from typing import Optional

app = FastAPI(title="Postman Master Builder")

# простой in-memory лок, чтобы не запускать два раза параллельно
_lock = threading.Lock()
_is_running = False

RUNNER_TOKEN = os.getenv("RUNNER_TOKEN")  # если задан, нужен заголовок X-Runner-Token

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run")
def run_build(x_runner_token: Optional[str] = Header(default=None)):
    global _is_running

    if RUNNER_TOKEN and x_runner_token != RUNNER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not os.getenv("POSTMAN_API_KEY"):
        raise HTTPException(status_code=500, detail="POSTMAN_API_KEY is not set")

    # на платформах будем отключать локальный venv/установку pip: USE_VENV=0
    env = os.environ.copy()
    env.setdefault("USE_VENV", "0")  # в облаке
    # если у тебя config.py читает ACTIVE_PROFILE — не надо, run_all сам переключает

    with _lock:
        if _is_running:
            raise HTTPException(status_code=409, detail="Already running")
        _is_running = True
    try:
        # вызываем твой run_all.py и собираем stdout/stderr
        proc = subprocess.run(
            ["python", "run_all.py"],
            env=env,
            capture_output=True,
            text=True
        )
        ok = proc.returncode == 0
        # урежем лог, чтобы ответ не раздулся
        tail = 8000
        return {
            "ok": ok,
            "returncode": proc.returncode,
            "stdout_tail": proc.stdout[-tail:] if proc.stdout else "",
            "stderr_tail": proc.stderr[-tail:] if proc.stderr else "",
        }
    finally:
        _is_running = False
