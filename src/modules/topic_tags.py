import re

from modules.word_freq import tokenize

_KEYWORDS_HEADER_RE = re.compile(r"[Kk]eywords?\s*[:\-]?\s*\n")
_ABSTRACT_HEADER_RE = re.compile(r"\n\s*[Aa]\s*[Bb]\s*[Ss]\s*[Tt]\s*[Rr]\s*[Aa]\s*[Cc]\s*[Tt]\s*\n")


def extract_intrinsic_keywords(full_text: str, max_keywords: int = 12) -> list[str]:
    """Extracts the article's own Keywords field.

    ScienceDirect PDFs list one keyword per line, immediately followed by
    an "abstract"/"a b s t r a c t" header line -- verified against all 280
    real PDFs in articles/, not the semicolon-separated "term1; term2"
    shape the design spec assumed before checking real extracted text.
    """
    match = _KEYWORDS_HEADER_RE.search(full_text)
    if match is None:
        return []

    keywords: list[str] = []
    for line in full_text[match.end():].split("\n"):
        stripped = line.strip()
        if not stripped or stripped.replace(" ", "").lower() == "abstract":
            break
        keywords.append(stripped)
        if len(keywords) >= max_keywords:
            break
    return keywords


def extract_abstract_text(full_text: str, max_chars: int = 2000) -> str | None:
    """Extracts the text immediately following the Abstract header, if present."""
    match = _ABSTRACT_HEADER_RE.search(full_text)
    if match is None:
        return None
    return full_text[match.end():match.end() + max_chars].strip()


def fallback_keywords_from_abstract(abstract_text: str, top_n: int = 5) -> list[str]:
    """Ranks the abstract's tokenize() output by frequency as a stand-in for
    "top TF-IDF terms" -- the corpus-wide tf_idf SQL table doesn't exist yet
    at this pipeline stage (Phase 2's C++ engine builds it after topic
    tagging runs), so this uses local term frequency only."""
    freq = tokenize(abstract_text)
    ranked = sorted(freq.items(), key=lambda item: (-item[1], item[0]))
    return [word for word, _ in ranked[:top_n]]
