import nltk


def download_corpora() -> None:
    for corpus in ("punkt", "punkt_tab", "stopwords"):
        nltk.download(corpus)


if __name__ == "__main__":
    download_corpora()
