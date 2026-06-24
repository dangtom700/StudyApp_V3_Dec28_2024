from pathlib import Path

from config import ARTICLES_DIR, DATA_DIR, DB_PATH, TOKEN_FREQ_DIR


def test_articles_dir_points_at_real_corpus():
    assert ARTICLES_DIR.name == "articles"
    assert ARTICLES_DIR.is_dir()
    assert any(ARTICLES_DIR.glob("*.pdf"))


def test_data_paths_are_under_data_dir():
    assert DB_PATH.parent == DATA_DIR
    assert TOKEN_FREQ_DIR.parent == DATA_DIR
    assert DB_PATH.name == "studyapp.db"
    assert TOKEN_FREQ_DIR.name == "token_freq"
