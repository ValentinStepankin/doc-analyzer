"""
export_csv.py — выгрузка результатов анализа в CSV напрямую из БД.

Дублирует GET /api/export/csv, но работает без запущенного сервера.
Используется если нужен экспорт из терминала, не поднимая api.py.

Использование:
    python export_csv.py                    # сохранить в data/export/
    python export_csv.py /path/to/out.csv   # указать путь вручную
"""

import csv
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "index.db"


def export(db_path: Path, out_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT
            path,
            ext,
            size,
            value_score,
            category,
            suggested_action,
            summary,
            why,
            status,
            processed_at
        FROM files
        WHERE status = 'ok'
        ORDER BY value_score DESC, path
    """).fetchall()

    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "path", "ext", "size", "value_score",
            "category", "suggested_action", "summary", "why",
            "status", "processed_at",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    return len(rows)


def main() -> None:
    if len(sys.argv) > 1:
        out_path = Path(sys.argv[1])
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = BASE_DIR / "data" / "export" / f"results_{stamp}.csv"

    if not DB_PATH.exists():
        print(f"ОШИБКА: база данных не найдена: {DB_PATH}")
        sys.exit(1)

    n = export(DB_PATH, out_path)
    print(f"Экспортировано {n} файл(ов) → {out_path}")


if __name__ == "__main__":
    main()
