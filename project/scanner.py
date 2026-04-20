"""
scanner.py — обход директорий с генерацией событий для каждого файла.

Оптимизирован для огромных архивов (100+ ТБ):
- Для проверки пропуска использует только метаданные ОС (путь, размер, mtime).
- Хеш файла считается только тогда, когда подтверждена необходимость обработки.
"""

import hashlib
import os
from pathlib import Path
from typing import Generator

import storage

SUPPORTED_EXTENSIONS = {
    # Текстовые файлы
    ".txt", ".md", ".csv", ".html", ".htm",
    # Документы
    ".pdf", ".docx", ".pptx", ".xlsx", ".xls",
    # Изображения
    ".jpg", ".jpeg", ".png", ".webp", ".tiff", ".tif",
}


def count_files(directories: list) -> int:
    """Быстрый подсчёт файлов с поддерживаемыми расширениями без чтения содержимого."""
    total = 0
    for directory in directories:
        dir_path = Path(directory).expanduser().resolve()
        if not dir_path.is_dir():
            continue
        for _, _, files in os.walk(dir_path):
            for f in files:
                if Path(f).suffix.lower() in SUPPORTED_EXTENSIONS:
                    total += 1
    return total


def scan_files(
    directories: list,
    conn,
) -> Generator[dict, None, None]:
    """
    Генерирует словари с типом события для каждого найденного файла:

      {'event': 'skip',    'path': ..., 'size': ..., 'mtime': ..., 'ext': ...}
      {'event': 'process', 'path': ..., 'size': ..., 'mtime': ..., 'ext': ..., 'hash': ...}
      {'event': 'error',   'path': ..., 'error': ...}

    Логика пропуска (файл не читается до подтверждения):
      1. Получить path + size + mtime от ОС.
      2. SELECT в SQLite: совпадают ли path, size и mtime?
      3. ДА  → пропустить немедленно.
      4. НЕТ → прочитать файл, посчитать хеш, вернуть 'process'.
    """
    for directory in directories:
        dir_path = Path(directory).expanduser().resolve()

        if not dir_path.exists():
            yield {"event": "error", "path": str(dir_path), "error": "Директория не найдена"}
            continue

        if not dir_path.is_dir():
            yield {"event": "error", "path": str(dir_path), "error": "Путь не является директорией"}
            continue

        for file_path in dir_path.rglob("*"):
            if not file_path.is_file():
                continue

            ext = file_path.suffix.lower()
            if ext not in SUPPORTED_EXTENSIONS:
                continue

            path_str = str(file_path)

            try:
                stat = file_path.stat()
            except OSError as e:
                yield {"event": "error", "path": path_str, "error": str(e)}
                continue

            size = stat.st_size
            mtime = str(stat.st_mtime)
            ext_clean = ext.lstrip(".")

            # Быстрая проверка по метаданным — диск не читается
            if storage.file_exists(conn, path_str, size, mtime):
                yield {
                    "event": "skip",
                    "path": path_str,
                    "size": size,
                    "mtime": mtime,
                    "ext": ext_clean,
                }
                continue

            # Новый или изменённый файл — считаем хеш
            try:
                file_hash = _compute_hash(file_path)
            except OSError as e:
                yield {"event": "error", "path": path_str, "error": f"Ошибка хеширования: {e}"}
                continue

            yield {
                "event": "process",
                "path": path_str,
                "size": size,
                "mtime": mtime,
                "ext": ext_clean,
                "hash": file_hash,
            }


def _compute_hash(file_path: Path) -> str:
    """SHA-256 хеш с потоковым чтением блоками по 64 КБ."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65_536), b""):
            h.update(block)
    return h.hexdigest()
