"""
extractor.py — извлечение содержимого файлов по типу.

Правила:
- .txt / .md / .csv / .html        → читать напрямую (без внешних инструментов)
- .pdf / .docx / .pptx             → Docling (текст)
- .pdf (при process_embedded_images: true)
                                   → PyMuPDF: изображения ≥ 100×100 px → Qwen3-VL
- .jpg / .png / и др.              → Qwen3-VL через Ollama (один проход: OCR + описание)
  Если process_standalone_images: false → вернуть пустой текст (только метаданные)
"""

import base64
import json
import re
from pathlib import Path

TEXT_EXTENSIONS  = {".txt", ".md", ".csv", ".html", ".htm"}
DOC_EXTENSIONS   = {".pdf", ".docx", ".pptx"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".tiff", ".tif"}

# Минимальный размер embedded-изображения для обработки (px по каждой стороне).
# Отсеивает иконки, разделители, декоративные элементы.
_MIN_IMAGE_PX = 100


def extract(file_info: dict, config: dict) -> dict:
    """
    Возвращает единый текстовый объект:
    {
        path, ext, hash,
        raw_text,          # текст из документа (Docling / plain read)
        ocr_text,          # текст на изображении (Qwen3-VL, standalone)
        image_description, # описание сцены (Qwen3-VL, standalone)
        merged_text        # всё вместе — для анализа
    }
    """
    path = file_info["path"]
    ext  = "." + file_info["ext"].lower()

    result = {
        "path":              path,
        "ext":               file_info["ext"],
        "hash":              file_info["hash"],
        "raw_text":          "",
        "ocr_text":          "",
        "image_description": "",
        "merged_text":       "",
    }

    if ext in TEXT_EXTENSIONS:
        result["raw_text"] = _read_text(path)

    elif ext in DOC_EXTENSIONS:
        result["raw_text"] = _extract_with_docling(path)

        # Embedded изображения из PDF (другие форматы — в будущем)
        if ext == ".pdf" and config.get("process_embedded_images", True):
            embedded = _extract_pdf_images(path, config)
            if embedded:
                result["raw_text"] += "\n\n" + embedded

    elif ext in IMAGE_EXTENSIONS:
        if not config.get("process_standalone_images", True):
            return result  # только метаданные
        ocr, description = _call_qwen(path, config)
        result["ocr_text"]          = ocr
        result["image_description"] = description

    # Собрать merged_text
    parts = []
    if result["raw_text"]:
        parts.append(result["raw_text"])
    if result["ocr_text"]:
        parts.append(f"[OCR TEXT]\n{result['ocr_text']}")
    if result["image_description"]:
        parts.append(f"[IMAGE DESCRIPTION]\n{result['image_description']}")
    result["merged_text"] = "\n\n".join(parts)

    return result


# ---------------------------------------------------------------------------
# Текстовые файлы
# ---------------------------------------------------------------------------

def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Документы через Docling
# ---------------------------------------------------------------------------

def _extract_with_docling(path: str) -> str:
    try:
        from docling.document_converter import DocumentConverter
    except ImportError:
        raise RuntimeError("Docling не установлен. Выполните: pip install docling")

    converter = DocumentConverter()
    result = converter.convert(path)
    return result.document.export_to_markdown()


# ---------------------------------------------------------------------------
# Embedded-изображения из PDF через PyMuPDF
# ---------------------------------------------------------------------------

def _extract_pdf_images(path: str, config: dict) -> str:
    """
    Извлекает embedded-изображения из PDF, прогоняет каждое через Qwen3-VL.
    Возвращает строку с блоками описаний для вставки в raw_text.
    Изображения меньше _MIN_IMAGE_PX × _MIN_IMAGE_PX пропускаются.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise RuntimeError("PyMuPDF не установлен. Выполните: pip install PyMuPDF")

    doc = fitz.open(path)
    blocks = []

    for page_num, page in enumerate(doc, start=1):
        for img_info in page.get_images(full=False):
            xref = img_info[0]

            try:
                pix = fitz.Pixmap(doc, xref)

                # Пропустить слишком маленькие изображения
                if pix.width < _MIN_IMAGE_PX or pix.height < _MIN_IMAGE_PX:
                    continue

                # Конвертировать CMYK → RGB (Qwen3-VL ожидает RGB/RGBA)
                if pix.n - pix.alpha >= 4:
                    pix = fitz.Pixmap(fitz.csRGB, pix)

                img_bytes = pix.tobytes("png")

            except Exception:
                # Повреждённое или нечитаемое изображение — пропускаем
                continue

            # Отправить в Qwen3-VL
            ocr, description = _call_qwen_bytes(img_bytes, config)

            if not ocr and not description:
                continue

            block_lines = [f"[Изображение, стр. {page_num}]"]
            if ocr:
                block_lines.append(f"OCR: {ocr}")
            if description:
                block_lines.append(f"Описание: {description}")
            blocks.append("\n".join(block_lines))

    doc.close()
    return "\n\n".join(blocks)


# ---------------------------------------------------------------------------
# Qwen3-VL через Ollama
# ---------------------------------------------------------------------------

def _call_qwen(path: str, config: dict) -> tuple:
    """Standalone-файл: читает с диска и отправляет в модель."""
    with open(path, "rb") as f:
        image_bytes = f.read()
    return _call_qwen_bytes(image_bytes, config)


def _call_qwen_bytes(image_bytes: bytes, config: dict) -> tuple:
    """Отправляет изображение (bytes) в Qwen3-VL. Возвращает (ocr_text, image_description)."""
    import requests

    prompt_path = Path(__file__).parent / "prompts" / "describe_image.txt"
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt = f.read()

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")

    ollama = config["ollama"]
    response = requests.post(
        f"{ollama.get('base_url', 'http://localhost:11434')}/api/generate",
        json={
            "model":  ollama["model_name"],
            "prompt": prompt,
            "images": [image_b64],
            "stream": False,
        },
        timeout=ollama.get("timeout", 3600),
    )
    response.raise_for_status()

    raw = response.json().get("response", "")
    return _parse_image_response(raw)


def _parse_image_response(raw: str) -> tuple:
    """Разобрать JSON из ответа модели → (ocr_text, image_description)."""
    try:
        data = json.loads(raw.strip())
        return str(data.get("ocr_text", "")), str(data.get("image_description", ""))
    except (json.JSONDecodeError, AttributeError):
        pass

    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group())
            return str(data.get("ocr_text", "")), str(data.get("image_description", ""))
        except json.JSONDecodeError:
            pass

    return "", raw.strip()
