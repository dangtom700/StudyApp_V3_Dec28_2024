from modules.word_freq import tokenize


def tokenize_prompt(text: str) -> dict[str, int]:
    """Thin pass-through to modules.word_freq.tokenize -- exists only so
    C++'s prompt-ranking path never reimplements NLTK/Porter-stemming/
    stopword logic for free-text queries."""
    return tokenize(text)
