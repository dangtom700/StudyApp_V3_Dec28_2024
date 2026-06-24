from modules.word_freq import tokenize


def test_tokenize_stems_and_counts_words():
    text = (
        "Control systems control models. The model offers control of "
        "control systems, et al. 2023."
    )
    result = tokenize(text)
    assert result == {"control": 4, "system": 2, "model": 2, "offer": 1}


def test_tokenize_drops_stopwords_only_input():
    assert tokenize("a an the") == {}


def test_tokenize_drops_repeated_char_tokens():
    assert tokenize("aaaaaa loooong wooord") == {}
