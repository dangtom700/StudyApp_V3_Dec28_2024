import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from modules.tokenize_prompt import tokenize_prompt
from modules.word_freq import tokenize


def test_tokenize_prompt_matches_word_freq_tokenize():
    text = "Model predictive control of nonlinear systems with control constraints."
    assert tokenize_prompt(text) == tokenize(text)
