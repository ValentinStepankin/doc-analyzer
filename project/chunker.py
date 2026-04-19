"""
chunker.py — нарезка извлечённого текста на чанки.

Правила:
- Изображения      → 1 файл = 1 чанк, не дробить
- PPTX             → нарезка по слайдам (разделители в markdown от Docling)
- Всё остальное    → сначала по структуре (заголовки/абзацы),
                     затем по размеру, если блок слишком большой
"""

import re

IMAGE_EXTENSIONS = {"jpg", "jpeg", "png", "webp", "tiff", "tif"}

DEFAULT_MIN_SIZE = 1500
DEFAULT_MAX_SIZE = 3000
DEFAULT_OVERLAP = 200


def chunk(extracted: dict, config: dict) -> list:
    """
    Возвращает список словарей: [{'text': '...', 'index': N}, ...]
    """
    ext = extracted.get("ext", "").lower()
    cfg = config.get("chunking", {})
    min_size = cfg.get("min_size", DEFAULT_MIN_SIZE)
    max_size = cfg.get("max_size", DEFAULT_MAX_SIZE)
    overlap = cfg.get("overlap", DEFAULT_OVERLAP)

    # Изображения: один объект, не делить
    if ext in IMAGE_EXTENSIONS:
        merged = (extracted.get("merged_text") or "").strip()
        if not merged:
            return []
        return [{"text": merged, "index": 0}]

    # PPTX: нарезка по слайдам
    if ext == "pptx":
        raw = (extracted.get("raw_text") or "").strip()
        if not raw:
            return []
        return _chunk_by_slides(raw)

    # Все остальные типы: по структуре, затем по размеру
    text = (extracted.get("raw_text") or extracted.get("merged_text") or "").strip()
    if not text:
        return []
    return _chunk_by_structure(text, min_size, max_size, overlap)


# ---------------------------------------------------------------------------
# PPTX: нарезка по слайдам
# ---------------------------------------------------------------------------

def _chunk_by_slides(text: str) -> list:
    """
    Docling экспортирует PPTX с границами слайдов как '---' или заголовками '## '.
    Сначала пробуем '---'; если нет — делим по заголовкам первого/второго уровня.
    """
    if re.search(r"\n\s*---\s*\n", text):
        slides = re.split(r"\n\s*---\s*\n", text)
    else:
        # Разделить *перед* каждым заголовком, сохраняя его в чанке
        slides = re.split(r"(?=\n#{1,2} )", text)

    chunks = []
    for i, slide in enumerate(slides):
        slide = slide.strip()
        if slide:
            chunks.append({"text": slide, "index": i})

    # Запасной вариант: весь текст как один чанк
    return chunks if chunks else [{"text": text, "index": 0}]


# ---------------------------------------------------------------------------
# Нарезка по структуре (для txt, md, html, docx, pdf)
# ---------------------------------------------------------------------------

def _chunk_by_structure(text: str, min_size: int, max_size: int, overlap: int) -> list:
    """
    1. Разбить на смысловые блоки (заголовки / пустые строки).
    2. Сгруппировать блоки в чанки, не превышающие max_size.
    3. Если отдельный блок превышает max_size — нарезать по размеру с перекрытием.
    """
    blocks = _split_into_blocks(text)

    chunks: list = []
    current = ""
    idx = 0

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # Блок сам по себе слишком большой → сбросить текущий, нарезать блок
        if len(block) > max_size:
            if current.strip():
                chunks.append({"text": current.strip(), "index": idx})
                idx += 1
                current = ""
            for sub in _split_by_size(block, max_size, overlap):
                chunks.append({"text": sub, "index": idx})
                idx += 1
            continue

        sep = "\n\n" if current else ""
        if current and len(current) + len(sep) + len(block) > max_size:
            # Сбросить текущий чанк
            chunks.append({"text": current.strip(), "index": idx})
            idx += 1
            # Перенести хвост с перекрытием в следующий чанк
            tail = current[-overlap:] if len(current) > overlap else current
            current = tail.strip() + "\n\n" + block
        else:
            current += sep + block

    if current.strip():
        chunks.append({"text": current.strip(), "index": idx})

    return chunks


def _split_into_blocks(text: str) -> list:
    """Разбить текст по пустым строкам или markdown-заголовкам."""
    # Разделить на 2+ переводах строки ИЛИ перед строкой-заголовком
    parts = re.split(r"\n{2,}|(?=\n#{1,6} )", text)
    return [p for p in parts if p.strip()]


def _split_by_size(text: str, max_size: int, overlap: int) -> list:
    """Разбить большой блок на части по max_size символов с перекрытием."""
    result = []
    start = 0
    while start < len(text):
        end = start + max_size
        if end >= len(text):
            result.append(text[start:].strip())
            break
        # Попробовать разрезать по границе предложения
        break_at = text.rfind(". ", start, end)
        if break_at <= start:
            break_at = end
        else:
            break_at += 1  # включить точку
        result.append(text[start:break_at].strip())
        start = max(start + 1, break_at - overlap)
    return [r for r in result if r]
