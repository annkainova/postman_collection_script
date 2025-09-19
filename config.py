# -*- coding: utf-8 -*-
"""
Единый конфиг для сборки Postman-коллекций.
Секреты — только через переменные окружения.
"""

import os
from typing import Dict, Any

# Опционально подхватываем .env (если установлен python-dotenv)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# === Базовые HTTP настройки ===
API_BASE: str = os.getenv("POSTMAN_API_BASE", "https://api.getpostman.com")
API_KEY: str = os.getenv("POSTMAN_API_KEY", "")

def get_headers() -> Dict[str, str]:
    key = API_KEY
    if not key:
        # Можно бросить здесь ошибку, но удобнее — в месте первого HTTP-запроса
        # raise RuntimeError("POSTMAN_API_KEY не задан")
        pass
    return {"X-Api-Key": key, "Content-Type": "application/json"}

# === Идентификаторы рабочих пространств и коллекций ===
WORKSPACES = {
    "autocomplete-full": "914515d6-8224-4f49-8a70-340c12ffc880",
    "bad-main": "906599c3-a08a-4b98-8a19-7fdfe267b62e",
    "clients-bad-analysis": "ed6cb6ea-45b4-46bf-8ac1-8d87824d6023",
    "test": "71e37054-1291-42e3-8873-e016b0ec863b",
}

COLLECTIONS = {
    "BAD": "45638072-52cbe9a4-01aa-47d2-b084-9bc6450f1050",
    "Автозаполнение (Полная документация)": "46112485-b1843756-de4f-4a01-941a-461f12f9196c",
}

PROFILES = {
    "auto_full": {
        "master_uid": COLLECTIONS["Автозаполнение (Полная документация)"],
        "master_name": "Автозаполнение (Полная документация)",
        "workspace_id": WORKSPACES["autocomplete-full"],
    },
    "bad_main": {
        "master_uid": COLLECTIONS["BAD"],
        "master_name": "BAD",
        "workspace_id": WORKSPACES["clients-bad-analysis"],
    },
}

# === Активный профиль === 
# Вот тут меняем значения
# ACTIVE_PROFILE: str = "auto_full" – если тебе нужно перенести все коллекции Автозаполнения в 1 коллекцию Автозаполнение(Полная документация)  
# или 
# ACTIVE_PROFILE: str = "bad_main" – если тебе нужно перенести все рабочие пространства 1 рабочее пространство Bad-main

ACTIVE_PROFILE: str = os.getenv("ACTIVE_PROFILE", "bad_main")
if ACTIVE_PROFILE not in PROFILES:
    raise RuntimeError(f"Unknown ACTIVE_PROFILE={ACTIVE_PROFILE}. "
                        f"Допустимые: {', '.join(PROFILES.keys())}")

_profile = PROFILES[ACTIVE_PROFILE]

# === Дефолты, используемые скриптом ===
DEFAULT_MASTER_UID: str = os.getenv("MASTER_UID", _profile["master_uid"])  # UID мастер-коллекции
DEFAULT_MASTER_NAME: str = os.getenv("MASTER_NAME", _profile["master_name"])  # Имя мастера
DEFAULT_WORKSPACE_ID: str | None = _profile["workspace_id"]  # workspace-источник

DEFAULT_FOLDER_PREFIX: str = os.getenv("FOLDER_NAME_PREFIX", "")
DEFAULT_ADD_README: bool = os.getenv("ADD_README_ITEM", "0") == "1"
FALLBACK_CREATE_ON_PUT_ERROR: bool = os.getenv("FALLBACK_CREATE_ON_PUT_ERROR", "1") == "1"
DEFAULT_CONCURRENCY: int = int(os.getenv("CONCURRENCY", "8"))
DEFAULT_SKIP_UNCHANGED: bool = os.getenv("SKIP_IF_NO_CHANGES", "1") == "1"
DEFAULT_EXCLUDE_PREFIXES: list[str] = ["[HIDDEN]"]


# Удобный агрегатор (если где-то нужно всё сразу)
DEFAULTS: Dict[str, Any] = {
    "MASTER_UID": DEFAULT_MASTER_UID,
    "MASTER_NAME": DEFAULT_MASTER_NAME,
    "WORKSPACE_ID": DEFAULT_WORKSPACE_ID,
    "FOLDER_PREFIX": DEFAULT_FOLDER_PREFIX,
    "ADD_README": DEFAULT_ADD_README,
    "FALLBACK_CREATE_ON_PUT_ERROR": FALLBACK_CREATE_ON_PUT_ERROR,
    "CONCURRENCY": DEFAULT_CONCURRENCY,
    "SKIP_UNCHANGED": DEFAULT_SKIP_UNCHANGED,
    "EXCLUDE_PREFIXES": DEFAULT_EXCLUDE_PREFIXES,
}
