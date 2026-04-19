"""
analyzer.py — отправка чанка в Ollama и получение структурированной оценки.

Модель ОБЯЗАНА отвечать строгим JSON по схеме из evaluate_chunk.txt.

Разделение ответственности за ошибки:
- Ошибки соединения / HTTP (Ollama недоступен) → пробрасываются наверх;
  main._process_file поставит status='error', файл попадёт в --reprocess-errors.
- Ошибки парсинга ответа модели → _parse_evaluation возвращает заглушку
  с suggested_action='review' (файл помечается ok, но выводится на ручной просмотр).
"""

import json
import re
from pathlib import Path

VALID_ACTIONS = {"keep", "archive", "review", "trash_candidate"}

_FALLBACK = {
    "is_valuable": False,
    "value_score": 0,
    "category": "trash",
    "summary": "Анализ не выполнен",
    "why_valuable": "Не удалось получить корректный ответ от модели",
    "entities": [],
    "suggested_action": "review",
}


def analyze_chunk(text: str, config: dict) -> dict:
    """Отправить один чанк в Ollama; вернуть словарь с оценкой.

    Ошибки соединения и HTTP-ошибки пробрасываются наверх — вызывающий код
    (main._process_file) пометит файл как status='error', и он попадёт
    в повторную очередь при следующем --reprocess-errors.
    Ошибки парсинга ответа модели обрабатываются внутри _parse_evaluation.
    """
    import requests

    prompt_path = Path(__file__).parent / "prompts" / "evaluate_chunk.txt"
    with open(prompt_path, "r", encoding="utf-8") as f:
        template = f.read()

    prompt = template.replace("{text}", text)
    ollama = config["ollama"]

    response = requests.post(
        f"{ollama['base_url']}/api/generate",
        json={
            "model": ollama["model_name"],
            "prompt": prompt,
            "stream": False,
        },
        timeout=ollama.get("timeout", 600),
    )
    response.raise_for_status()
    raw = response.json().get("response", "")
    return _parse_evaluation(raw)


# ---------------------------------------------------------------------------
# Парсинг и валидация
# ---------------------------------------------------------------------------

def _parse_evaluation(raw: str) -> dict:
    """Разобрать JSON из ответа модели с двумя запасными стратегиями."""
    # 1. Прямой парсинг
    try:
        data = json.loads(raw.strip())
        return _validate(data)
    except (json.JSONDecodeError, AttributeError):
        pass

    # 2. Извлечь первый JSON-объект из ответа
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group())
            return _validate(data)
        except json.JSONDecodeError:
            pass

    # 3. Жёсткая заглушка
    result = _FALLBACK.copy()
    result["summary"] = raw[:200] if raw else "Пустой ответ"
    return result


def _validate(data: dict) -> dict:
    """Привести типы и убедиться в наличии обязательных полей."""
    score = int(data.get("value_score", 0))
    score = max(0, min(100, score))

    action = str(data.get("suggested_action", "review"))
    if action not in VALID_ACTIONS:
        action = "review"

    return {
        "is_valuable": bool(data.get("is_valuable", False)),
        "value_score": score,
        "category": str(data.get("category", "trash")),
        "summary": str(data.get("summary", "")),
        "why_valuable": str(data.get("why_valuable", "")),
        "entities": list(data.get("entities", [])),
        "suggested_action": action,
    }
