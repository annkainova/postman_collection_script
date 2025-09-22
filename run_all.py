#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
from pathlib import Path
USE_VENV = os.getenv("USE_VENV", "1") == "1"


PROJECT_ROOT = Path(__file__).resolve().parent
VENV_DIR = PROJECT_ROOT / ".venv"
IS_MAC = sys.platform == "darwin"

def venv_python() -> str:
    if IS_MAC or sys.platform.startswith("linux"):
        return str(VENV_DIR / "bin" / "python3")
    else:
        return str(VENV_DIR / "Scripts" / "python.exe")  # Windows fallback

def ensure_venv_and_deps():
    # 1) venv
    if not VENV_DIR.exists():
        print("→ Создаю виртуальное окружение .venv …")
        subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)

    py = venv_python()
    # 2) pip up-to-date (не обязательно, но полезно)
    subprocess.run([py, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=True)
    # 3) зависимости
    print("→ Устанавливаю зависимости (requests, python-dotenv) …")
    subprocess.run([py, "-m", "pip", "install", "requests", "python-dotenv"], check=True)

def run_profile(profile: str):
    env = os.environ.copy()
    env["ACTIVE_PROFILE"] = profile
    py = venv_python()
    print(f"\n=== Запуск профиля: {profile} ===\n")
    subprocess.run(
        [py, str(PROJECT_ROOT / "build_master_mass_merge.py"), "--all", "--skip-unchanged"],
        env=env,
        check=True,
    )

def main():
    ensure_venv_and_deps()
    # порядок фиксированный
    for profile in ["auto_full", "bad_main"]:
        run_profile(profile)
    print("\n✅ Готово: оба профиля собраны (auto_full → bad_main)")

if __name__ == "__main__":
    main()
