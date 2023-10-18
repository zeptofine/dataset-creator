import re

NUMBERS = re.compile(r"(\d+)")


def alphanumeric_sort(value: str) -> list[str | int]:
    """Key function to sort strings containing numbers by proper numerical order."""
    parts: list[str | int] = NUMBERS.split(value.upper())
    parts[1::2] = map(int, parts[1::2])  # type: ignore
    return parts
