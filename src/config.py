from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
REPO_ROOT = SRC_DIR.parent
PROJECT_ROOT = REPO_ROOT.parent

ARTICLES_DIR = PROJECT_ROOT / "articles"
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "studyapp.db"
TOKEN_FREQ_DIR = DATA_DIR / "token_freq"
DATAMUSE_CACHE_PATH = DATA_DIR / "datamuse_cache.json"
