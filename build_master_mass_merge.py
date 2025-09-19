import os
import sys
import json
import time
import hashlib
import argparse
from typing import List, Dict, Any, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import config as cfg

try:
    import requests
except ImportError:
    print("Установи пакет: pip install requests", file=sys.stderr)
    sys.exit(1)

# ===================== НАСТРОЙКИ ПО УМОЛЧАНИЮ =====================
API_BASE = "https://api.getpostman.com"
API_KEY: str = os.getenv("POSTMAN_API_KEY", "")
HEADERS = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}



# === Дефолты для запуска ===
DEFAULT_MASTER_UID = cfg.DEFAULT_MASTER_UID
DEFAULT_MASTER_NAME = cfg.DEFAULT_MASTER_NAME
DEFAULT_WORKSPACE_ID = cfg.DEFAULT_WORKSPACE_ID
DEFAULT_FOLDER_PREFIX = cfg.DEFAULT_FOLDER_PREFIX
DEFAULT_ADD_README = cfg.DEFAULT_ADD_README
DEFAULT_CONCURRENCY = cfg.DEFAULT_CONCURRENCY
DEFAULT_SKIP_UNCHANGED = cfg.DEFAULT_SKIP_UNCHANGED



# ===================== HTTP УТИЛИТЫ =====================

def _req(method: str, path: str, body: dict | None = None, retry: int = 3) -> dict:
    if not HEADERS.get("X-Api-Key"):
        raise RuntimeError("POSTMAN_API_KEY не задан")
    if not API_KEY:
        raise RuntimeError("POSTMAN_API_KEY не задан.")
    url = f"{API_BASE}{path}"
    backoff = 1.5
    for attempt in range(retry):
        try:
            if method == "GET":
                r = requests.get(url, headers=HEADERS, timeout=60)
            elif method == "POST":
                r = requests.post(url, headers=HEADERS, data=json.dumps(body) if body else None, timeout=120)
            elif method == "PUT":
                r = requests.put(url, headers=HEADERS, data=json.dumps(body) if body else None, timeout=120)
            else:
                raise RuntimeError(f"Unsupported method {method}")
        except requests.RequestException as e:
            if attempt < retry - 1:
                time.sleep(backoff); backoff *= 2; continue
            raise RuntimeError(f"HTTP error: {e}") from e

        if r.status_code in (429, 500, 502, 503, 504) and attempt < retry - 1:
            time.sleep(backoff); backoff *= 2; continue

        if not r.ok:
            raise RuntimeError(f"{method} {path} → {r.status_code} {r.text}")

        return r.json()
    raise RuntimeError("Unreachable")

# ===================== POSTMAN API =====================

def list_collections(workspace_id: str | None = None) -> List[Dict[str, Any]]:
    path = f"/collections?workspace={workspace_id}" if workspace_id else "/collections"
    data = _req("GET", path)
    return data.get("collections", [])


def get_collection(uid: str) -> Dict[str, Any]:
    data = _req("GET", f"/collections/{uid}")
    return data["collection"]


def create_collection(col_json: Dict[str, Any], workspace_id: str | None) -> Dict[str, Any]:
    path = f"/collections?workspace={workspace_id}" if workspace_id else "/collections"
    return _req("POST", path, {"collection": col_json})


def ensure_postman_id(col_json: dict, master_uid: str) -> None:
    """
    Тихо вытягиваем info._postman_id из существующей master и проставляем в col_json,
    чтобы PUT обновлял ту же коллекцию, а не создавал новую.
    """
    try:
        existing = get_collection(master_uid)
        pid = existing.get("info", {}).get("_postman_id")
        if pid:
            col_json.setdefault("info", {})["_postman_id"] = pid
    except Exception as e:
        print(f"warn: couldn't fetch existing master info: {e}")


def update_collection(uid: str, col_json: dict, workspace_id: str | None) -> dict:
    # 1) сохраним info._postman_id (для корректного PUT)
    ensure_postman_id(col_json, uid)

    # 2) полезная метрика: размер
    try:
        payload_size_kb = len(json.dumps({"collection": col_json}).encode("utf-8")) / 1024
        print(f"payload ~ {payload_size_kb:.1f} KB")
    except Exception:
        pass

    # 3) PUT, при 5xx → POST (создание новой)
    try:
        return _req("PUT", f"/collections/{uid}", {"collection": col_json})
    except RuntimeError as e:
        msg = str(e)
        if (" 500 " in msg or " 502 " in msg or " 503 " in msg or " 504 " in msg) and FALLBACK_CREATE_ON_PUT_ERROR:
            print("PUT failed with 5xx → fallback: creating a NEW collection via POST …")
            created = create_collection(col_json, workspace_id)
            try:
                new_uid = created.get("collection", {}).get("uid")
            except Exception:
                new_uid = None
            print(f"NEW master UID: {new_uid or '<unknown>'}  (при необходимости переопредели --master-uid)")
            return created
        raise

# ===================== НОРМАЛИЗАЦИЯ/САНИТАЙЗ =====================

def _normalize_description(desc: Any) -> Any:
    # Может быть строкой или {"content": "...", "type": "text/markdown"}
    return desc


def _sanitize_item(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит item к валидной форме Postman v2.1:
    - Если это группа (есть 'item'): рекурсивно чиним её элементы.
    - Если это запрос (есть 'request'): оставляем.
    - Если нет ни 'request', ни 'item' → превращаем в группу с пустым item[].
    """
    if "item" in obj:
        items = obj.get("item") or []
        fixed_children = []
        for child in items:
            if isinstance(child, dict):
                fixed_children.append(_sanitize_item(child))
            else:
                fixed_children.append({"name": str(child), "item": []})
        obj["item"] = fixed_children
        return obj

    if "request" in obj:
        return obj

    # Лист без request/item → делаем папкой
    new_obj = {"name": obj.get("name", "Untitled"), "item": []}
    if "description" in obj:
        new_obj["description"] = obj["description"]
    for k in ("event", "auth", "variable"):
        if k in obj:
            new_obj[k] = obj[k]
    return new_obj


def _scrub_ids_in_place(obj: Any, keep_root_info_postman_id: bool = False, _path: Tuple[str, ...] = ()) -> None:
    """Рекурсивно удаляет id/uid/_postman_id. Оставляет только корневой info._postman_id (если флаг True)."""
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            if k in ("id", "uid"):
                obj.pop(k, None)
            elif k == "_postman_id":
                if not (keep_root_info_postman_id and _path == ("info",)):
                    obj.pop(k, None)
        for k, v in list(obj.items()):
            _scrub_ids_in_place(v, keep_root_info_postman_id, _path + (k,))
    elif isinstance(obj, list):
        for v in obj:
            _scrub_ids_in_place(v, keep_root_info_postman_id, _path)


def _normalized_digest(obj: Any) -> str:
    """Возвращает SHA1 нормализованной структуры (без volatile-полей)."""
    import copy
    x = copy.deepcopy(obj)
    _scrub_ids_in_place(x, keep_root_info_postman_id=False)
    blob = json.dumps(x, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()

# ===================== СБОРКА МАСТЕР-КОЛЛЕКЦИИ =====================

def folder_from_collection(col: Dict[str, Any], folder_prefix: str, add_readme: bool) -> Dict[str, Any]:
    info = col.get("info", {}) or {}
    original_name = info.get("name", "Unnamed")
    folder_name = f"{folder_prefix}{original_name}"

    desc = _normalize_description(info.get("description"))
    items = (col.get("item") or [])[:]

    folder: Dict[str, Any] = {"name": folder_name, "item": items}
    if desc:
        folder["description"] = desc
        if add_readme:
            folder["item"] = [{"name": "📘 README", "description": desc, "item": []}] + folder["item"]

    if col.get("event"):
        folder["event"] = col["event"]
    if col.get("auth"):
        folder["auth"] = col["auth"]
    if col.get("variable"):
        folder["variable"] = col["variable"]

    folder = _sanitize_item(folder)
    return folder


def _dedupe_names(names: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for n in names:
        if n not in seen:
            seen[n] = 1
            out.append(n)
        else:
            k = seen[n]
            seen[n] += 1
            out.append(f"{n} ({k+1})")
    return out


def _get_existing_master_description(master_uid: str | None) -> str | None:
    """Возвращает info.description из текущей мастер-коллекции (если есть)."""
    if not master_uid:
        return None
    try:
        existing = get_collection(master_uid)
        return (existing.get("info") or {}).get("description")
    except Exception:
        return None


def build_master(
    source_cols: List[Dict[str, Any]],
    name: str,
    folder_prefix: str,
    add_readme: bool,
    master_description: str | None = None,
) -> Dict[str, Any]:
    folders = [folder_from_collection(c, folder_prefix, add_readme) for c in source_cols]

    # дедуплицируем одинаковые имена папок (как было)
    names = [f["name"] for f in folders]
    fixed_names = _dedupe_names(names)
    for i, nm in enumerate(fixed_names):
        folders[i]["name"] = nm

    info = {
        "name": name,
        "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
    }
    if master_description:  # если есть существующее описание — сохраняем
        info["description"] = master_description

    master = {"info": info, "item": folders}
    master["item"] = [_sanitize_item(it) for it in master["item"]]
    return master


# ===================== ФИЛЬТРЫ =====================

def should_include(name: str, include_prefixes: List[str] | None, exclude_prefixes: List[str] | None) -> bool:
    if include_prefixes:
        ok = any(name.startswith(p) for p in include_prefixes)
        if not ok:
            return False
    if exclude_prefixes:
        if any(name.startswith(p) for p in exclude_prefixes):
            return False
    return True

# ===================== ГЛАВНАЯ ЛОГИКА =====================

def _fetch_many(uids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for uid in uids:
        try:
            col = get_collection(uid)
            print(f"  • ok {uid}: {col.get('info', {}).get('name')}")
            out.append(col)
        except Exception as e:
            print(f"  • error {uid}: {e}")
    return out


def maybe_skip_put_if_unchanged(master_uid: str, new_master: Dict[str, Any]) -> bool:
    try:
        current = get_collection(master_uid)
    except Exception as e:
        print(f"warn: can't load existing master for diff: {e}")
        return False

    new_d = _normalized_digest(new_master)
    cur_d = _normalized_digest(current)
    print(f"digest existing={cur_d} new={new_d}")
    return new_d == cur_d


def run(
    workspace_id: str | None,
    master_uid: str | None,
    master_name: str,
    folder_prefix: str,
    add_readme: bool,
    use_all: bool,
    include_prefixes: List[str] | None,
    exclude_prefixes: List[str] | None,
    source_uids: List[str] | None,
    concurrency: int,
    skip_unchanged: bool,
    dry_run: bool,
) -> None:
    # Источники
    if use_all:
        cols_meta = list_collections(workspace_id)
        print(f"Найдено коллекций: {len(cols_meta)} (workspace={workspace_id or 'ALL'})")
        uids: List[str] = []
        for c in cols_meta:
            uid = c.get("uid"); name = c.get("name", "")
            if not uid:
                continue
            if master_uid and uid == master_uid:
                continue
            if should_include(name, include_prefixes, exclude_prefixes):
                uids.append(uid)
        if not uids:
            print("Нет источников после фильтрации.", file=sys.stderr)
            sys.exit(2)
        print(f"Отобрано источников: {len(uids)}")
    else:
        if not source_uids:
            print("Нужно либо --all, либо --source-uid (можно много раз)", file=sys.stderr)
            sys.exit(2)
        uids = source_uids
        print(f"Используем SOURCE_UIDS: {len(uids)} шт.")

    # Тянем источники
    sources = _fetch_many(uids)
    if not sources:
        print("Не удалось загрузить ни одной коллекции.", file=sys.stderr)
        sys.exit(3)

    # ВАЖНО: вытаскиваем текущее описание мастера (если мастер_uid задан)
    existing_desc = _get_existing_master_description(master_uid)

    # Собираем мастер
    master = build_master(
        sources,
        master_name,
        folder_prefix,
        add_readme,
        master_description=existing_desc,
    )
    # Чистим id/uid внутри, но СОХРАНЯЕМ корневой info._postman_id (его проставит ensure_postman_id перед PUT)
    _scrub_ids_in_place(master, keep_root_info_postman_id=True)

    # Выведем сводку
    summary = {
        "master_name": master["info"]["name"],
        "folders_count": len(master["item"]),
        "folders": [it.get("name") for it in master["item"]],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if dry_run:
        print("DRY-RUN: обновление не выполнялось.")
        return

    # Обновляем/создаём
    if master_uid:
        if skip_unchanged and maybe_skip_put_if_unchanged(master_uid, master):
            print("⏭️  Изменений нет — PUT пропущен.")
            return
        print(f"Обновляем мастер-коллекцию {master_uid} …")
        _ = update_collection(master_uid, master, workspace_id)
        print("✅ Обновлено.")
    else:
        print("Создаём новую мастер-коллекцию …")
        _ = create_collection(master, workspace_id)
        print("✅ Создано.")

# ===================== CLI =====================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge many Postman collections into a single master (folders)")
    p.add_argument("--workspace", default=DEFAULT_WORKSPACE_ID, help="ID воркспейса (если не задан, ищем по всем)")
    p.add_argument("--master-uid", default=DEFAULT_MASTER_UID, help="UID мастер-коллекции для PUT (иначе создадим новую)")
    p.add_argument("--name", default=DEFAULT_MASTER_NAME, help="Имя мастер-коллекции")
    p.add_argument("--prefix", default=DEFAULT_FOLDER_PREFIX, help="Префикс имён папок в мастере")
    p.add_argument("--add-readme", action="store_true", default=DEFAULT_ADD_README, help="Вставлять README-элемент в каждую папку")
    p.add_argument("--all", action="store_true", help="Автообнаружение всех коллекций в воркспейсе")
    p.add_argument("--include-prefix", action="append", default=None, help="Фильтр: включить только имена с данным префиксом (можно много раз)")
    p.add_argument("--exclude-prefix", action="append", default=cfg.DEFAULT_EXCLUDE_PREFIXES, help="Фильтр: исключить имена с данным префиксом (можно много раз)")
    p.add_argument("--source-uid", action="append", default=None, help="UID исходной коллекции (можно много раз)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Количество параллельных запросов")
    p.add_argument("--skip-unchanged", action="store_true", default=DEFAULT_SKIP_UNCHANGED, help="Пропускать PUT, если изменений нет")
    p.add_argument("--dry-run", action="store_true", help="Не отправлять изменения (только показать сводку)")

    return p.parse_args()


def main():
    args = parse_args()
    run(
        workspace_id=args.workspace,
        master_uid=args.master_uid or None,
        master_name=args.name,
        folder_prefix=args.prefix,
        add_readme=args.add_readme,
        use_all=args.all,
        include_prefixes=args.include_prefix,
        exclude_prefixes=args.exclude_prefix,
        source_uids=args.source_uid,
        concurrency=max(1, args.concurrency),
        skip_unchanged=args.skip_unchanged,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
