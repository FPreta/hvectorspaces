from contextlib import redirect_stderr
from gensim.models import Word2Vec
from gensim.models.callbacks import CallbackAny2Vec
from gensim.models.phrases import Phrases, Phraser
from gensim.utils import simple_preprocess
from pathlib import Path

import multiprocessing as mp
import os
import sys
import time

def ngram_builder(corpus:list[list[str]], min_count:str=5, threshold:str=5, delimiter:str="_") -> list[list[str]]:
    ngram = Phrases(
        corpus,
        min_count=5,
        threshold=5,
        delimiter="_",   # <-- str, not bytes
    )
    phraser = Phraser(ngram)
    corpus_ngram = [phraser[doc] for doc in corpus]
    return corpus_ngram

# ---------- (A) Optional: write your tokenized docs to disk (recommended if big) ----------
def write_corpus(tokens_iter, out_path: str):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for doc in tokens_iter:
            if not doc:
                continue
            f.write(" ".join(doc) + "\n")

class EpochLogger(CallbackAny2Vec):
    def __init__(self, total_epochs):
        self.total_epochs = total_epochs
        self.epoch = 0
        self.start_time = time.time()

    def on_epoch_end(self, model):
        self.epoch += 1
        elapsed = time.time() - self.start_time
        avg_epoch = elapsed / self.epoch
        remaining = avg_epoch * (self.total_epochs - self.epoch)

        print(
            f"Epoch {self.epoch}/{self.total_epochs} | "
            f"elapsed: {elapsed/60:.1f} min | "
            f"ETA: {remaining/60:.1f} min"
        )
        
def fit_model(
    sentences,
    model_type: str = "word2vec",  # "word2vec" | "fasttext"
    vector_size: int = 300,
    window: int = 5,
    min_count: int = 10,
    sg: int = 1,
    negative: int = 10,
    sample: float = 1e-4,
    epochs: int = 10,
    workers: int | None = None,
    callbacks=None,
    # fasttext-specific
    min_n: int = 3,
    max_n: int = 6,
):
    if workers is None:
        workers = max(1, mp.cpu_count() - 1)

    Model = Word2Vec if model_type.lower() == "word2vec" else FastText

    with open(os.devnull, "w") as dn, redirect_stderr(dn):
        model = Model(
            sentences=sentences,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            sg=sg,
            negative=negative,
            sample=sample,
            epochs=epochs,
            workers=workers,
            callbacks=callbacks,
            **(
                dict(min_n=min_n, max_n=max_n)
                if model_type.lower() == "fasttext"
                else {}
            ),
        )

    return model


def save_model(
    model,
    out_dir: str | Path,
    model_name: str,
    save_full: bool = True,
    save_kv: bool = True,
    save_vec: bool = True,
    binary: bool = False,
):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if save_full:
        model.save(str(out_dir / f"{model_name}.model"))

    if save_kv:
        model.wv.save(str(out_dir / f"{model_name}.kv"))

    if save_vec:
        model.wv.save_word2vec_format(
            str(out_dir / f"{model_name}.vec"),
            binary=binary
        )