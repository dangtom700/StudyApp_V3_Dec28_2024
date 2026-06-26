import json

from modules.datamuse import expand_keyword, load_cache, save_cache


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_expand_keyword_calls_datamuse_and_populates_cache(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        return _FakeResponse([{"word": "operate"}, {"word": "command"}])

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)
    monkeypatch.setattr("modules.datamuse.time.sleep", lambda seconds: None)

    cache = {}
    result = expand_keyword("control", cache, limit=8)

    assert result == ["operate", "command"]
    assert cache["control"] == ["operate", "command"]
    assert calls == [{"ml": "control", "max": 8}]


def test_expand_keyword_uses_cache_without_calling_network(monkeypatch):
    def fake_get(*args, **kwargs):
        raise AssertionError("should not call network when cache hit")

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)

    cache = {"control": ["operate", "command"]}
    result = expand_keyword("Control", cache)  # different case, same normalized key

    assert result == ["operate", "command"]


def test_expand_keyword_returns_empty_list_on_request_failure(monkeypatch):
    import requests

    def fake_get(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr("modules.datamuse.requests.get", fake_get)
    monkeypatch.setattr("modules.datamuse.time.sleep", lambda seconds: None)

    cache = {}
    result = expand_keyword("control", cache)

    assert result == []
    assert cache["control"] == []


def test_load_and_save_cache_round_trip(tmp_path):
    cache_path = tmp_path / "datamuse_cache.json"
    save_cache({"control": ["operate", "command"]}, cache_path)

    loaded = load_cache(cache_path)

    assert loaded == {"control": ["operate", "command"]}
    assert json.loads(cache_path.read_text(encoding="utf-8")) == {
        "control": ["operate", "command"]
    }


def test_load_cache_returns_empty_dict_when_file_missing(tmp_path):
    assert load_cache(tmp_path / "missing.json") == {}


def test_expand_keyword_real_network_smoke_test():
    """One real call against the live Datamuse API to verify the actual
    integration works end-to-end, not just the mocked call shape. Asserts
    structure (non-empty list of strings) rather than exact words/scores --
    unlike this corpus's deterministic local PDF/NLTK pipeline, a live
    third-party API's index isn't ours to pin exact values against."""
    cache = {}
    result = expand_keyword("control", cache, limit=5)

    assert len(result) >= 1
    assert all(isinstance(word, str) and word for word in result)
    assert cache["control"] == result
