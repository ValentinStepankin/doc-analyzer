"""
api.py — FastAPI-сервер для doc-analyzer.

Запуск:
    python api.py
    uvicorn api:app --reload  # для разработки

Открывает браузер автоматически на http://localhost:8000
"""

import csv
import io
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests as http_requests
import yaml
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR     = Path(__file__).parent
CONFIG_PATH  = BASE_DIR / "config.yaml"
DB_PATH      = BASE_DIR / "data" / "index.db"
STATUS_PATH  = BASE_DIR / "data" / "logs" / "status.json"
EXPORT_DIR         = BASE_DIR / "data" / "export"
FRONTEND_DIR       = BASE_DIR / "frontend"
PID_FILE           = BASE_DIR / "data" / "_scan.pid"
RECENT_PATHS_FILE  = BASE_DIR / "data" / "recent_paths.json"
MAX_RECENT_PATHS   = 5

app = FastAPI(title="doc-analyzer API")

# Текущий процесс сканирования (один в любой момент времени)
_scan_process: Optional[subprocess.Popen] = None
_scan_lock = threading.Lock()   # защита от одновременного запуска двух процессов


# ─── PID helpers ──────────────────────────────────────────────────────────────

def _write_pid(pid: int) -> None:
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))

def _read_pid() -> Optional[int]:
    try:
        return int(PID_FILE.read_text().strip())
    except Exception:
        return None

def _clear_pid() -> None:
    PID_FILE.unlink(missing_ok=True)

def _pid_alive(pid: int) -> bool:
    """Проверить существование процесса без его завершения."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ─── Recent paths helpers ─────────────────────────────────────────────────────

def _load_recent_paths() -> list:
    try:
        return json.loads(RECENT_PATHS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

def _save_recent_path(path: str) -> None:
    paths = [p for p in _load_recent_paths() if p != path]
    paths.insert(0, path)
    paths = paths[:MAX_RECENT_PATHS]
    RECENT_PATHS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RECENT_PATHS_FILE.write_text(
        json.dumps(paths, ensure_ascii=False), encoding="utf-8"
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _scan_running() -> bool:
    global _scan_process
    # Проверить in-memory процесс
    if _scan_process is not None:
        if _scan_process.poll() is None:
            return True
        _scan_process = None  # процесс завершился
    # Fallback: PID-файл (сервер мог перезапуститься)
    pid = _read_pid()
    if pid and _pid_alive(pid):
        return True
    _clear_pid()
    return False


# ─── Status ───────────────────────────────────────────────────────────────────

@app.get("/api/status")
def get_status():
    """Текущий прогресс сканирования из status.json."""
    if STATUS_PATH.exists():
        with open(STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Если status.json говорит что сканирование идёт, но процесса нет —
        # это устаревший файл (прошлый запуск). Обнуляем current_file.
        if data.get("current_file") and not _scan_running():
            data["current_file"] = None
        return data
    return {
        "total_found": 0, "processed": 0,
        "skipped": 0, "errors": 0, "current_file": None,
    }


# ─── Stats ────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats():
    """Агрегаты для карточек Обзора и распределений."""
    if not DB_PATH.exists():
        return {
            "total": 0, "processed": 0, "errors": 0, "skipped": 0,
            "by_action": {}, "by_category": {}, "recent": [],
        }

    conn = get_conn()
    try:
        total     = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        processed = conn.execute("SELECT COUNT(*) FROM files WHERE status='ok'").fetchone()[0]
        errors    = conn.execute("SELECT COUNT(*) FROM files WHERE status='error'").fetchone()[0]
        pending   = conn.execute("SELECT COUNT(*) FROM files WHERE status='pending'").fetchone()[0]

        by_action = {
            r["suggested_action"]: r["cnt"]
            for r in conn.execute(
                """SELECT suggested_action, COUNT(*) AS cnt
                   FROM files WHERE status='ok' AND suggested_action IS NOT NULL
                   GROUP BY suggested_action"""
            ).fetchall()
        }

        by_category = {
            r["category"]: r["cnt"]
            for r in conn.execute(
                """SELECT category, COUNT(*) AS cnt
                   FROM files WHERE status='ok' AND category IS NOT NULL
                   GROUP BY category ORDER BY cnt DESC"""
            ).fetchall()
        }

        recent = [
            dict(r) for r in conn.execute(
                """SELECT id, path, ext, value_score, category, suggested_action, processed_at
                   FROM files WHERE status='ok' AND processed_at IS NOT NULL
                   ORDER BY processed_at DESC LIMIT 10"""
            ).fetchall()
        ]

        return {
            "total": total,
            "processed": processed,
            "errors": errors,
            "skipped": None,   # берётся из status.json, не из БД
            "by_action": by_action,
            "by_category": by_category,
            "recent": recent,
        }
    finally:
        conn.close()


# ─── Ollama health ────────────────────────────────────────────────────────────

@app.get("/api/ollama/health")
def ollama_health():
    """Проверить доступность Ollama."""
    config = load_config()
    ollama = config.get("ollama", {})
    base_url = ollama.get("base_url", "http://localhost:11434")
    model    = ollama.get("model_name", "")
    try:
        r = http_requests.get(f"{base_url}/api/tags", timeout=3)
        r.raise_for_status()
        return {"online": True, "model": model}
    except Exception:
        return {"online": False, "model": model}


# ─── Files ────────────────────────────────────────────────────────────────────

_ALLOWED_SORT = {"value_score", "path", "ext", "size", "category", "processed_at"}


@app.get("/api/files")
def list_files(
    action:    str = Query(default=""),
    category:  str = Query(default=""),
    ext:       str = Query(default=""),
    min_score: int = Query(default=0),
    sort:      str = Query(default="value_score"),
    order:     str = Query(default="desc"),
    page:      int = Query(default=1, ge=1),
    limit:     int = Query(default=50, ge=1, le=100),
    status:    str = Query(default=""),   # '' = ok only | 'error' = errors | 'all' = все
):
    if not DB_PATH.exists():
        return {"files": [], "total": 0, "page": page, "limit": limit}

    conn = get_conn()
    try:
        if status == "error":
            conditions = ["status = 'error'"]
        elif status == "pending":
            conditions = ["status = 'pending'"]
        elif status == "all":
            conditions = []
        else:
            conditions = ["status = 'ok'"]
        params: list = []
        if action:    conditions.append("suggested_action = ?"); params.append(action)
        if category:  conditions.append("category = ?");         params.append(category)
        if ext:       conditions.append("ext = ?");              params.append(ext)
        if min_score: conditions.append("value_score >= ?");     params.append(min_score)

        where     = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sort_col  = sort if sort in _ALLOWED_SORT else "value_score"
        order_sql = "ASC" if order.lower() == "asc" else "DESC"

        total = conn.execute(f"SELECT COUNT(*) FROM files {where}", params).fetchone()[0]

        offset = (page - 1) * limit
        rows = conn.execute(
            f"""SELECT id, path, ext, size, value_score, category,
                       suggested_action, summary, why, processed_at, status
                FROM files {where}
                ORDER BY {sort_col} {order_sql}
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

        return {"files": [dict(r) for r in rows], "total": total, "page": page, "limit": limit}
    finally:
        conn.close()


@app.get("/api/files/{file_id}")
def get_file(file_id: int):
    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="File not found")

        chunks = conn.execute(
            """SELECT id, chunk_index, summary, value_score, category, entities_json
               FROM chunks WHERE file_id = ? ORDER BY chunk_index""",
            (file_id,),
        ).fetchall()

        result = dict(row)
        result["chunks"] = [dict(c) for c in chunks]
        return result
    finally:
        conn.close()


class PatchFileBody(BaseModel):
    suggested_action: str = ""
    category: str = ""


@app.patch("/api/files/{file_id}")
def patch_file(file_id: int, body: PatchFileBody):
    valid_actions = {"keep", "archive", "review", "trash_candidate"}
    if body.suggested_action and body.suggested_action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"suggested_action must be one of: {valid_actions}")

    if not body.suggested_action and not body.category:
        raise HTTPException(status_code=400, detail="Provide suggested_action or category")

    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    conn = get_conn()
    try:
        cursor = None
        if body.suggested_action:
            cursor = conn.execute("UPDATE files SET suggested_action = ? WHERE id = ?", (body.suggested_action, file_id))
        if body.category:
            cursor = conn.execute("UPDATE files SET category = ? WHERE id = ?", (body.category, file_id))
        conn.commit()
        if cursor and cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="File not found")
        return {"ok": True}
    finally:
        conn.close()


# ─── Search ───────────────────────────────────────────────────────────────────

@app.get("/api/search")
def search(
    q:     str = Query(default=""),
    page:  int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
):
    if not q.strip() or not DB_PATH.exists():
        return {"files": [], "total": 0, "page": page, "limit": limit}

    conn = get_conn()
    try:
        offset = (page - 1) * limit

        total_row = conn.execute(
            """SELECT COUNT(*) FROM search_index si
               JOIN files f ON f.id = CAST(si.file_id AS INTEGER)
               WHERE search_index MATCH ?""",
            (q,),
        ).fetchone()
        total = total_row[0] if total_row else 0

        rows = conn.execute(
            """SELECT f.id, f.path, f.ext, f.size, f.value_score, f.category,
                      f.suggested_action, f.summary, f.processed_at
               FROM search_index si
               JOIN files f ON f.id = CAST(si.file_id AS INTEGER)
               WHERE search_index MATCH ?
               ORDER BY rank
               LIMIT ? OFFSET ?""",
            (q, limit, offset),
        ).fetchall()
        files = [dict(r) for r in rows]
        return {"files": files, "total": total, "page": page, "limit": limit}
    except sqlite3.OperationalError:
        # Некорректный FTS5-запрос от пользователя — возвращаем пустой результат
        return {"files": [], "total": 0, "page": page, "limit": limit}
    finally:
        conn.close()


# ─── Browse directory ─────────────────────────────────────────────────────────

@app.post("/api/browse/directory")
def browse_directory():
    """Открыть нативный диалог выбора папки. Работает на macOS и Windows."""
    import platform
    system = platform.system()
    path = None

    try:
        if system == "Darwin":
            # AppleScript: нативный диалог macOS
            result = subprocess.run(
                ["osascript", "-e", "POSIX path of (choose folder)"],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                path = result.stdout.strip().rstrip("/")

        elif system == "Windows":
            # PowerShell + WinForms: нативный диалог Windows
            # $form.TopMost = $true + ShowDialog($form) — диалог поверх браузера
            # [Console]::OutputEncoding = UTF8 — корректная передача кириллицы
            ps = (
                "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
                "Add-Type -AssemblyName System.Windows.Forms; "
                "$form = New-Object System.Windows.Forms.Form; "
                "$form.TopMost = $true; "
                "$form.ShowInTaskbar = $false; "
                "$form.WindowState = 'Minimized'; "
                "$form.Show(); "
                "$d = New-Object System.Windows.Forms.FolderBrowserDialog; "
                "$d.Description = 'Выберите папку для сканирования'; "
                "$d.RootFolder = 'MyComputer'; "
                "if ($d.ShowDialog($form) -eq [System.Windows.Forms.DialogResult]::OK)"
                " { $d.SelectedPath } else { '' }; "
                "$form.Dispose()"
            )
            result = subprocess.run(
                ["powershell", "-STA", "-NoProfile", "-Command", ps],
                capture_output=True, text=True, timeout=120, encoding="utf-8",
            )
            if result.returncode == 0:
                path = result.stdout.strip() or None

        else:
            # Linux: zenity (опционально)
            result = subprocess.run(
                ["zenity", "--file-selection", "--directory", "--title=Выберите папку"],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                path = result.stdout.strip() or None

    except subprocess.TimeoutExpired:
        pass  # пользователь отменил диалог
    except FileNotFoundError:
        raise HTTPException(status_code=501, detail="Диалог выбора папки недоступен на этой ОС")

    return {"path": path}


# ─── Recent paths ─────────────────────────────────────────────────────────────

@app.get("/api/recent-paths")
def get_recent_paths():
    return {"paths": _load_recent_paths()}


@app.delete("/api/recent-paths")
def delete_recent_path(path: str = Query()):
    paths = [p for p in _load_recent_paths() if p != path]
    RECENT_PATHS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RECENT_PATHS_FILE.write_text(json.dumps(paths, ensure_ascii=False), encoding="utf-8")
    return {"paths": paths}


# ─── Scan ─────────────────────────────────────────────────────────────────────

class ScanStartBody(BaseModel):
    directory:                 str
    process_standalone_images: bool = True
    process_embedded_images:   bool = True
    model_name:                str  = ""


@app.post("/api/scan/start")
def scan_start(body: ScanStartBody):
    global _scan_process

    if not Path(body.directory).is_dir():
        raise HTTPException(status_code=400, detail=f"Директория не найдена: {body.directory}")

    with _scan_lock:
        if _scan_running():
            raise HTTPException(status_code=409, detail="Scan already running")

        cmd = [sys.executable, str(BASE_DIR / "main.py"), body.directory]
        if body.model_name:
            cmd += ["--model", body.model_name]
        if not body.process_standalone_images:
            cmd.append("--no-standalone-images")
        if not body.process_embedded_images:
            cmd.append("--no-embedded-images")
        _scan_process = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        _write_pid(_scan_process.pid)
        _save_recent_path(body.directory)
        return {"ok": True, "pid": _scan_process.pid}


@app.post("/api/scan/stop")
def scan_stop():
    global _scan_process
    stopped = False

    if _scan_process and _scan_process.poll() is None:
        _scan_process.terminate()
        _scan_process = None
        stopped = True
    else:
        pid = _read_pid()
        if pid and _pid_alive(pid):
            os.kill(pid, signal.SIGTERM)
            stopped = True

    _clear_pid()
    if stopped:
        return {"ok": True}
    return {"ok": False, "detail": "No active scan"}


# ─── Export ───────────────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(filter: str = Query(default="")):
    """Генерирует CSV из БД и отдаёт как скачиваемый файл."""
    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    conn = get_conn()
    try:
        conditions = ["status = 'ok'"]
        params: list = []
        if filter in {"keep", "archive", "review", "trash_candidate"}:
            conditions.append("suggested_action = ?")
            params.append(filter)

        where = " AND ".join(conditions)
        rows = conn.execute(
            f"""SELECT path, ext, size, value_score, category, suggested_action,
                       summary, why, status, processed_at
                FROM files WHERE {where}
                ORDER BY value_score DESC, path""",
            params,
        ).fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "path", "ext", "size", "value_score", "category",
        "suggested_action", "summary", "why", "status", "processed_at",
    ])
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))

    stamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname    = f"results_{stamp}.csv"
    content  = buf.getvalue().encode("utf-8-sig")

    # Сохранить копию в data/export/
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(EXPORT_DIR / fname, "wb") as f:
        f.write(content)

    return Response(
        content=content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/export/history")
def export_history():
    """Список ранее сохранённых CSV-файлов."""
    if not EXPORT_DIR.exists():
        return {"files": []}
    files = sorted(EXPORT_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {"files": [{"name": f.name} for f in files[:20]]}


@app.get("/api/export/download/{name}")
def export_download(name: str):
    """Скачать конкретный CSV по имени файла."""
    if "/" in name or "\\" in name or ".." in name:
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = EXPORT_DIR / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(path), media_type="text/csv", filename=name)


# ─── Static files (должен быть последним) ────────────────────────────────────

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time
    import webbrowser

    import uvicorn

    def _open_browser():
        time.sleep(1.2)
        webbrowser.open("http://localhost:8000")

    threading.Thread(target=_open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
