from config import ARTICLES_DIR
from modules.extract_text import extract_article
from modules.topic_tags import (
    extract_abstract_text,
    extract_intrinsic_keywords,
    fallback_keywords_from_abstract,
)


def test_extract_intrinsic_keywords_six_keyword_fixture():
    article = extract_article(
        ARTICLES_DIR / "A-closed-loop-control-strategy-for-automotive-EHB-base_2026_Control-Engineer.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == [
        "EHB systems",
        "Hysteresis characteristics",
        "RC operator",
        "RBF neural network",
        "Closed-loop control",
        "Stability proof",
    ]


def test_extract_intrinsic_keywords_nine_keyword_fixture():
    article = extract_article(
        ARTICLES_DIR / "A-data-driven-approach-for-tailored-fatigue-modeling-_2026_Computers---Indus.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == [
        "Physical fatigue",
        "Industrial engineering",
        "Wearable sensors",
        "Human factors",
        "Fuzzy model",
        "Fatigue-recovery model",
        "Data-driven approach",
        "Production planning and control",
        "Industry 5.0",
    ]


def test_extract_intrinsic_keywords_returns_empty_when_no_keywords_field():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    assert extract_intrinsic_keywords(article.full_text) == []


def test_extract_abstract_text_finds_plain_abstract_header():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    abstract = extract_abstract_text(article.full_text)
    assert abstract is not None
    assert abstract.startswith(
        "This study investigates on the Drag-Free and Attitude Control System"
    )


def test_extract_abstract_text_returns_none_when_absent():
    assert extract_abstract_text("no abstract header anywhere in this text") is None


def test_fallback_keywords_from_abstract_ranks_by_frequency():
    article = extract_article(
        ARTICLES_DIR / "Dynamic-modeling--linearization-and-characteristic-anal_2026_Chinese-Journal.pdf"
    )
    abstract = extract_abstract_text(article.full_text)
    assert fallback_keywords_from_abstract(abstract, top_n=5) == [
        "model",
        "system",
        "dynam",
        "linear",
        "analysi",
    ]
