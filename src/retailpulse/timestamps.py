"""Shared UTC contract for local and Spark duplicate selection."""
import re
from datetime import datetime, timezone


def normalise_timestamp(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?", value):
        raise ValueError("updated_at must be an ISO timestamp with seconds and optional UTC offset")
    if re.search(r"[+-]\d{2}:\d{2}$", value) and int(value[-2:]) >= 60:
        raise ValueError("Invalid timestamp offset minutes")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def tie_columns(columns, key):
    """Fixed alphabetical source-column order, excluding key and normalised timestamp."""
    return sorted(set(columns) - {key, "updated_at"})
