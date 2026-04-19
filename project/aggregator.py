"""
aggregator.py — сборка результата уровня файла из оценок отдельных чанков.

Правила:
- value_score          = максимум среди всех чанков
- category / suggested_action / why = берётся из лучшего чанка
- summary              = сводка лучшего чанка; для многочанковых файлов — топ-3 через ' | '
- entities             = дедуплицированное объединение по всем чанкам
"""


def aggregate(chunk_evaluations: list, file_info: dict) -> dict:
    if not chunk_evaluations:
        return {
            "summary": "Содержимое не извлечено",
            "value_score": 0,
            "category": "trash",
            "suggested_action": "trash_candidate",
            "why": "Из файла не удалось извлечь текст",
        }

    # Лучший чанк = наибольший value_score
    best = max(chunk_evaluations, key=lambda c: c.get("value_score", 0))

    value_score = best.get("value_score", 0)
    category = best.get("category", "trash")
    suggested_action = best.get("suggested_action", "review")
    why = best.get("why_valuable", "")

    # Сводка: один чанк → использовать напрямую; несколько → объединить топ-3
    if len(chunk_evaluations) == 1:
        summary = best.get("summary", "")
    else:
        top = sorted(chunk_evaluations, key=lambda c: c.get("value_score", 0), reverse=True)[:3]
        parts = [c.get("summary", "") for c in top if c.get("summary")]
        summary = " | ".join(parts) if parts else best.get("summary", "")

    # Сущности: дедуплицированное объединение с сохранением порядка
    seen: set = set()
    entities: list = []
    for chunk in chunk_evaluations:
        for entity in chunk.get("entities", []):
            if entity and entity not in seen:
                seen.add(entity)
                entities.append(entity)

    return {
        "summary": summary,
        "value_score": value_score,
        "category": category,
        "suggested_action": suggested_action,
        "why": why,
        "entities": entities,
    }
