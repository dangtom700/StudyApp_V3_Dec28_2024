import re
from collections import defaultdict

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Words V2 found needed manual exclusion beyond NLTK's stopword list (mostly
# modal verbs that don't discriminate topic), plus "et"/"al" which pollute
# academic-article word frequencies via "et al." citations.
_EXTRA_STOPWORDS = {
    "also", "could", "done", "enough", "far", "get", "got", "gotten", "may",
    "might", "must", "near", "need", "ought", "shall", "since", "theirselves",
    "us", "would", "et", "al",
}

_REPEATED_CHAR_RE = re.compile(r"([a-zA-Z])\1{2,}")
_NON_WORD_RE = re.compile(r"[^\w\s]")

_stemmer = PorterStemmer()
_stop_words = frozenset(stopwords.words("english")) | _EXTRA_STOPWORDS


def tokenize(text: str) -> dict[str, int]:
    text = _NON_WORD_RE.sub("", text).lower()
    tokens = nltk.word_tokenize(text)

    freq: dict[str, int] = defaultdict(int)
    for token in tokens:
        if (
            token.isalpha()
            and token not in _stop_words
            and not _REPEATED_CHAR_RE.search(token)
        ):
            freq[_stemmer.stem(token)] += 1

    return dict(freq)
