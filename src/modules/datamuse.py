import json
import time
from pathlib import Path

import requests

DATAMUSE_URL = "https://api.datamuse.com/words"


def load_cache(cache_path: Path) -> dict[str, list[str]]:
    if not cache_path.exists():
        return {}
    return json.loads(cache_path.read_text(encoding="utf-8"))


def save_cache(cache: dict[str, list[str]], cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def expand_keyword(
    keyword: str,
    cache: dict[str, list[str]],
    limit: int = 8,
    request_delay: float = 0.1,
) -> list[str]:
    """Returns Datamuse's "means like" related words for a keyword, reusing
    `cache` (keyed by lowercased keyword) across calls so articles sharing
    common domain vocabulary -- verified common in this corpus, e.g. "model
    predictive control" -- never re-hit the network for the same term."""
    key = keyword.strip().lower()
    if key in cache:
        return cache[key]

    try:
        response = requests.get(DATAMUSE_URL, params={"ml": key, "max": limit}, timeout=10)
        response.raise_for_status()
        related = [item["word"] for item in response.json()]
    except requests.RequestException:
        related = []

    cache[key] = related
    time.sleep(request_delay)
    return related
