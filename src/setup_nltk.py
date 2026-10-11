import nltk


def download_corpora() -> None:
    for corpus in ("punkt", "punkt_tab", "stopwords"):
        print(f"Downloading NLTK corpus: {corpus} ... ", end="", flush=True)
        ok = nltk.download(corpus, quiet=True)
        print("ok" if ok else "FAILED", flush=True)


if __name__ == "__main__":
    download_corpora()
