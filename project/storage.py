"""
storage.py — операции с SQLite.
Таблицы: files, chunks, search_index (FTS5).
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path


def init_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _create_tables(conn)
    return conn


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            path             TEXT UNIQUE NOT NULL,
            hash             TEXT,
            ext              TEXT,
            size             INTEGER,
            mtime            TEXT,
            status           TEXT DEFAULT 'pending',
            summary          TEXT,
            value_score      INTEGER,
            category         TEXT,
            suggested_action TEXT,
            why              TEXT,
            processed_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS chunks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id      INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            chunk_index  INTEGER NOT NULL,
            text         TEXT,
            summary      TEXT,
            value_score  INTEGER,
            category     TEXT,
            entities_json TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
            file_id    UNINDEXED,
            path,
            summary,
            category,
            text,
            tokenize = "unicode61"
        );

    """)
    conn.commit()



def file_exists(conn: sqlite3.Connection, path: str, size: int, mtime: str) -> bool:
    """Быстрая проверка по метаданным ОС — файл с диска не читается.
    Пропускает только файлы с status='ok': ошибочные и незавершённые будут обработаны повторно."""
    row = conn.execute(
        "SELECT id FROM files WHERE path = ? AND size = ? AND mtime = ? AND status = 'ok'",
        (path, size, mtime),
    ).fetchone()
    return row is not None


def insert_file_metadata(
    conn: sqlite3.Connection,
    path: str,
    hash_: str,
    ext: str,
    size: int,
    mtime: str,
) -> int:
    """Upsert метаданных файла. Устанавливает status='pending'. Возвращает file_id."""
    now = datetime.now().isoformat()
    # INSERT OR IGNORE сохраняет существующий id; UPDATE обновляет метаданные и сбрасывает статус в pending.
    conn.execute(
        "INSERT OR IGNORE INTO files (path, hash, ext, size, mtime, status, processed_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        (path, hash_, ext, size, mtime, now),
    )
    conn.execute(
        "UPDATE files SET hash=?, ext=?, size=?, mtime=?, status='pending', processed_at=? WHERE path=?",
        (hash_, ext, size, mtime, now, path),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM files WHERE path=?", (path,)).fetchone()
    return row["id"]


def update_file_result(
    conn: sqlite3.Connection,
    file_id: int,
    summary: str,
    value_score: int,
    category: str,
    suggested_action: str,
    why: str,
) -> None:
    conn.execute(
        """UPDATE files
           SET summary=?, value_score=?, category=?, suggested_action=?, why=?,
               status='ok', processed_at=?
           WHERE id=?""",
        (summary, value_score, category, suggested_action, why,
         datetime.now().isoformat(), file_id),
    )
    conn.commit()


def insert_chunk(
    conn: sqlite3.Connection,
    file_id: int,
    chunk_index: int,
    text: str,
    summary: str,
    value_score: int,
    category: str,
    entities: list,
) -> None:
    conn.execute(
        """INSERT INTO chunks (file_id, chunk_index, text, summary, value_score, category, entities_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (file_id, chunk_index, text, summary, value_score, category, json.dumps(entities, ensure_ascii=False)),
    )
    conn.commit()


def delete_file_chunks(conn: sqlite3.Connection, file_id: int) -> None:
    conn.execute("DELETE FROM chunks WHERE file_id = ?", (file_id,))
    conn.commit()


def mark_file_ok(conn: sqlite3.Connection, file_id: int) -> None:
    """Отметить файл как успешно обработанный (только метаданные, без полного анализа)."""
    conn.execute("UPDATE files SET status = 'ok' WHERE id = ?", (file_id,))
    conn.commit()


def mark_file_error(conn: sqlite3.Connection, file_id: int) -> None:
    """Отметить файл как обработанный с ошибкой — будет повторён при --reprocess-errors."""
    conn.execute("UPDATE files SET status = 'error' WHERE id = ?", (file_id,))
    conn.commit()


def reset_errors(conn: sqlite3.Connection) -> int:
    """Сбросить статус ошибочных файлов в 'pending' для повторной обработки.
    Возвращает количество затронутых файлов."""
    cursor = conn.execute("UPDATE files SET status = 'pending' WHERE status = 'error'")
    conn.commit()
    return cursor.rowcount


def update_search_index(
    conn: sqlite3.Connection,
    file_id: int,
    path: str,
    summary: str,
    category: str,
    full_text: str,
) -> None:
    """Пересобрать FTS5-запись для данного файла."""
    # Удалить устаревшую запись
    conn.execute("DELETE FROM search_index WHERE file_id = ?", (str(file_id),))
    # Обрезать текст, чтобы не раздувать FTS-индекс (100 тыс. символов достаточно для поиска)
    text_preview = (full_text or "")[:100_000]
    conn.execute(
        "INSERT INTO search_index (file_id, path, summary, category, text) VALUES (?, ?, ?, ?, ?)",
        (str(file_id), path, summary or "", category or "", text_preview),
    )
    conn.commit()
