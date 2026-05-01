import math


def validate_positive_price(value, *, field_name, context):
    """Проверяет, что цена является положительным конечным числом."""
    if value is None:
        return f"Некорректная цена: {context}, field={field_name}, value={value}"

    if isinstance(value, bool):
        return f"Некорректная цена: {context}, field={field_name}, value={value}"

    if not isinstance(value, (int, float)):
        return f"Некорректная цена: {context}, field={field_name}, value={value}"

    numeric_value = float(value)

    if not math.isfinite(numeric_value):
        return f"Некорректная цена: {context}, field={field_name}, value={value}"

    if numeric_value <= 0:
        return f"Некорректная цена: {context}, field={field_name}, value={value}"

    return None
