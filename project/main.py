"""
main.py — оркестратор doc-analyzer.

Использование:
    python main.py                              # пути берутся из config.yaml
    python main.py /path/to/dir1 /path2        # переопределить пути через CLI
    python main.py --config /other/config.yaml /path/to/dir
    python main.py --model gemma3:4b /path     # переопределить модель
    python main.py --no-standalone-images      # пропустить standalone-изображения
    python main.py --no-embedded-images        # пропустить embedded-изображения в PDF
    python main.py --reprocess-errors          # повторить файлы с ошибками

Пайплайн для каждого файла:
    scanner → extractor → chunker → analyzer → aggregator → storage

Логирование:
    - Каждое событие (обработан / пропущен / ошибка) логируется сразу
    - Сводка выводится каждые N файлов (настраивается)
    - data/logs/status.json обновляется после каждого события
"""

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# Базовая директория — папка с этим скриптом
BASE_DIR = Path(__file__).parent


def load_config(config_path: str) -> dict:
    try:
        import yaml
    except ImportError:
        raise RuntimeError("PyYAML не установлен. Выполните: pip install pyyaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_path(config: dict, key: str, default: str) -> Path:
    """Разрешить путь из конфига относительно BASE_DIR."""
    raw = config.get(key, default)
    p = Path(raw)
    return p if p.is_absolute() else BASE_DIR / p


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger("doc-analyzer")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    # Консоль: INFO и выше, построчный вывод
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # Файл: DEBUG и выше (полный трейс)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def write_status(status_path: Path, status: dict) -> None:
    try:
        status_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = status_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        tmp.replace(status_path)  # атомарная замена
    except Exception:
        pass  # сбой записи статуса не должен ронять основной процесс


def _get_resources() -> dict:
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        return {
            "cpu_percent":  round(cpu, 1),
            "ram_used_mb":  round(ram.used  / 1024 / 1024),
            "ram_total_mb": round(ram.total / 1024 / 1024),
        }
    except ImportError:
        return {}


def print_summary(logger: logging.Logger, status: dict) -> None:
    res = _get_resources()
    res_str = (
        f"  |  CPU={res['cpu_percent']}%  "
        f"RAM={res['ram_used_mb']}MB/{res['ram_total_mb']}MB"
        if res else ""
    )
    logger.info(
        "--- СВОДКА  найдено=%d  обработано=%d  пропущено=%d  ошибок=%d%s ---",
        status["total_found"], status["processed"], status["skipped"], status["errors"],
        res_str,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="doc-analyzer — локальный пайплайн анализа файлов")
    parser.add_argument("paths", nargs="*", help="Директории для сканирования (переопределяет config)")
    parser.add_argument("--config", default=str(BASE_DIR / "config.yaml"), help="Путь к config.yaml")
    parser.add_argument("--model", default="", help="Переопределить model_name из config.yaml")
    parser.add_argument("--no-standalone-images", action="store_true", help="Отключить обработку standalone-изображений")
    parser.add_argument("--no-embedded-images", action="store_true", help="Отключить обработку embedded-изображений в PDF")
    parser.add_argument(
        "--reprocess-errors",
        action="store_true",
        help="Повторно обработать файлы, завершившиеся с ошибкой при предыдущем запуске",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    if args.paths:
        config["scan_paths"] = args.paths
    if args.model:
        config.setdefault("ollama", {})["model_name"] = args.model
    if args.no_standalone_images:
        config["process_standalone_images"] = False
    if args.no_embedded_images:
        config["process_embedded_images"] = False

    if not config.get("ollama", {}).get("model_name"):
        print("ОШИБКА: model_name не задан. Передайте --model <имя> или выберите модель в веб-интерфейсе.", file=sys.stderr)
        sys.exit(1)

    scan_paths = config.get("scan_paths", [])
    if not scan_paths:
        print("ОШИБКА: scan_paths не настроен. Отредактируйте config.yaml или передайте пути аргументами.", file=sys.stderr)
        sys.exit(1)

    # Разрешить пути
    db_path = resolve_path(config.get("database", {}), "path", "data/index.db")
    log_cfg = config.get("logging", {})
    log_dir = BASE_DIR / log_cfg.get("log_dir", "data/logs")
    status_path = BASE_DIR / log_cfg.get("status_file", "data/logs/status.json")
    summary_interval = log_cfg.get("summary_interval", 100)

    logger = setup_logging(log_dir)

    import aggregator
    import analyzer
    import chunker
    import extractor
    import scanner
    import storage

    logger.info("=" * 60)
    logger.info("doc-analyzer запущен")
    logger.info("Модель       : %s", config["ollama"].get("model_name"))
    logger.info("Ollama URL   : %s", config["ollama"].get("base_url", "http://localhost:11434"))
    logger.info("Фото файлы   : %s", "включены" if config.get("process_standalone_images", True) else "ОТКЛЮЧЕНЫ")
    logger.info("Фото в PDF   : %s", "включены" if config.get("process_embedded_images", True) else "ОТКЛЮЧЕНЫ")
    logger.info("Директории   : %s", scan_paths)
    logger.info("База данных  : %s", db_path)
    logger.info("=" * 60)

    conn = storage.init_db(str(db_path))

    if args.reprocess_errors:
        n = storage.reset_errors(conn)
        logger.info("--reprocess-errors: %d файл(ов) сброшено в очередь повторной обработки", n)

    logger.info("Подсчёт файлов...")
    total_files = scanner.count_files(scan_paths)
    logger.info("Файлов в очереди: %d", total_files)

    status = {
        "total_found": total_files,
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "current_file": None,
        "started_at": datetime.now().isoformat(),
    }
    write_status(status_path, status)

    try:
        for file_event in scanner.scan_files(scan_paths, conn):
            event_type = file_event["event"]
            path = file_event["path"]

            status["current_file"] = path
            write_status(status_path, status)

            # ── ПРОПУСК ───────────────────────────────────────────────────
            if event_type == "skip":
                status["skipped"] += 1
                logger.debug("SKIP  %s", path)
                write_status(status_path, status)

            # ── ОШИБКА СКАНИРОВАНИЯ ───────────────────────────────────────
            elif event_type == "error":
                status["errors"] += 1
                logger.warning("ERROR %s  →  %s", path, file_event.get("error"))
                write_status(status_path, status)

            # ── ОБРАБОТКА ─────────────────────────────────────────────────
            elif event_type == "process":
                logger.info("PROCESS  %s", path)
                try:
                    _process_file(file_event, config, conn, logger,
                                  storage, extractor, chunker, analyzer, aggregator)
                    status["processed"] += 1
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    status["errors"] += 1
                    logger.error("  FAILED  %s  →  %s", path, exc)
                    logger.debug(traceback.format_exc())
                finally:
                    res = _get_resources()
                    if res:
                        status.update(res)
                    write_status(status_path, status)

            # ── ПЕРИОДИЧЕСКАЯ СВОДКА ──────────────────────────────────────
            done = status["processed"] + status["skipped"] + status["errors"]
            if done > 0 and done % summary_interval == 0:
                print_summary(logger, status)

    except KeyboardInterrupt:
        logger.info("\nПрервано пользователем (Ctrl+C) — прогресс сохранён, можно продолжить.")
    finally:
        status["current_file"] = None
        status["finished_at"] = datetime.now().isoformat()
        write_status(status_path, status)
        print_summary(logger, status)
        conn.close()


def _process_file(
    file_event: dict,
    config: dict,
    conn,
    logger,
    storage,
    extractor,
    chunker,
    analyzer,
    aggregator,
) -> None:
    path = file_event["path"]
    image_exts = {"jpg", "jpeg", "png", "webp", "tiff", "tif"}

    t_file = time.time()
    file_id = None
    try:
        # 1. Сохранить метаданные (upsert) — устанавливает status='pending'
        file_id = storage.insert_file_metadata(
            conn,
            path,
            file_event["hash"],
            file_event["ext"],
            file_event["size"],
            file_event["mtime"],
        )

        # 2. Standalone-изображения при отключённом флаге → только метаданные
        if file_event["ext"] in image_exts and not config.get("process_standalone_images", True):
            logger.info("  → обработка standalone-изображений отключена, метаданные сохранены")
            storage.mark_file_ok(conn, file_id)
            return

        # 3. Извлечь текст
        t0 = time.time()
        extracted = extractor.extract(file_event, config)
        logger.debug("  → извлечение: %.0fs", time.time() - t0)

        if not (extracted.get("merged_text") or "").strip():
            logger.warning("  → текст не извлечён, только метаданные")
            storage.mark_file_ok(conn, file_id)
            return

        # 4. Нарезать на чанки
        chunks = chunker.chunk(extracted, config)
        logger.info("  → %d чанк(ов)", len(chunks))

        if not chunks:
            logger.warning("  → чанки не сформированы")
            storage.mark_file_ok(conn, file_id)
            return

        # 5. Проанализировать каждый чанк
        storage.delete_file_chunks(conn, file_id)
        chunk_results = []

        for chunk in chunks:
            idx   = chunk["index"] + 1
            total = len(chunks)
            logger.info("    чанк %d/%d  (%d симв.) — запрос к модели...", idx, total, len(chunk["text"]))
            t0 = time.time()
            evaluation = analyzer.analyze_chunk(chunk["text"], config)
            logger.info("    чанк %d/%d  готов за %.0fs", idx, total, time.time() - t0)
            storage.insert_chunk(
                conn,
                file_id,
                chunk["index"],
                chunk["text"],
                evaluation["summary"],
                evaluation["value_score"],
                evaluation["category"],
                evaluation["entities"],
            )
            chunk_results.append(evaluation)

        # 6. Агрегировать до уровня файла
        file_result = aggregator.aggregate(chunk_results, file_event)

        # 7. Сохранить итоговый результат файла (status='ok' устанавливается внутри)
        storage.update_file_result(
            conn,
            file_id,
            file_result["summary"],
            file_result["value_score"],
            file_result["category"],
            file_result["suggested_action"],
            file_result["why"],
        )

        # 8. Обновить индекс полнотекстового поиска
        storage.update_search_index(
            conn,
            file_id,
            path,
            file_result["summary"],
            file_result["category"],
            extracted["merged_text"],
        )

        elapsed = time.time() - t_file
        elapsed_str = f"{int(elapsed // 60)}m{int(elapsed % 60):02d}s" if elapsed >= 60 else f"{elapsed:.0f}s"
        logger.info(
            "  → %s  score=%-3d  action=%-16s  %s",
            elapsed_str,
            file_result["value_score"],
            file_result["suggested_action"],
            file_result["summary"][:80],
        )

    except KeyboardInterrupt:
        raise
    except Exception:
        if file_id is not None:
            storage.mark_file_error(conn, file_id)
        raise


if __name__ == "__main__":
    main()
