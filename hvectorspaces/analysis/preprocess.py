import re
import spacy
import unicodedata

from collections import Counter
from spacy.lang.en import English
from spacy.tokens.doc import Doc

def merge_title_and_abstract(entry:dict[str,str]):
    if isinstance(entry['abstract'], str):
        if isinstance(entry['title'], str):
            return entry['title'] + ".\n" + entry['abstract']
        else:
            return entry['abstract']
    elif isinstance(entry['title'], str): 
        return entry['title']
    else:
        return None

def remove_links(text: str) -> str:    
    URL_PATTERN = re.compile(
        r"""(
            https?://\S+        |   # http:// or https://
            www\.\S+           |   # www.
            \b[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\S*  # bare domains
        )""",
        re.VERBOSE,
    )

    return URL_PATTERN.sub(" ", text)


def clean_openalex_text(text: str) -> str:
    # Unicode normalization
    text = unicodedata.normalize("NFKC", text)

    # Remove links EARLY
    text = remove_links(text)

    # Remove zero-width and BOM chars
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)

    # Remove LaTeX math blocks
    text = re.sub(r"\$[^$]+\$", " ", text)

    # Remove LaTeX commands (\alpha, \mathbb{R}, etc.)
    text = re.sub(r"\\[a-zA-Z]+(\{[^}]*\})?", " ", text)

    # Replace math operators & symbols with space
    text = re.sub(r"[∑∂≈⊗⊕≤≥≠∞√±×÷]", " ", text)

    # Collapse repeated punctuation
    text = re.sub(r"([=+\-*_]){2,}", " ", text)

    # Keep only letters, numbers, basic punctuation
    text = re.sub(r"[^0-9A-Za-zÀ-ÿ.,;:()\-\s]", " ", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text

def top_hyphenated_words(
    corpus: dict[int, list[str]],
    topn: int = 50,
) -> list[tuple[str, int]]:
    
    HYPHEN_PATTERN = re.compile(r"\b[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+\b")
    counter = Counter()

    for docs in corpus.values():
        for doc in docs:
            counter.update(HYPHEN_PATTERN.findall(doc.lower()))

    return counter.most_common(topn)

def print_counts(corpus: dict[int, list[str]], terms: list[str]) -> None:
    pattern = re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape, terms)), re.I)
    counts = dict.fromkeys(terms, 0)

    for docs in corpus.values():
        for doc in docs:
            for m in pattern.findall(doc):
                counts[m.lower()] += 1

    for t in terms:
        print(f"{t}: {counts[t]}")
        
def replace_hyphen_terms(
    corpus: dict[int, list[str]],
    terms: list[str],
    repl: str = "_",
) -> dict[int, list[str]]:
    
    PATTERN = re.compile(r"\b\w+(?:-\w+)+\b")
    terms = {t.lower() for t in terms}
    
    def repl_fn(match: re.Match):
        w = match.group(0)
        return w.replace("-", repl) if w.lower() in terms else w

    out = {}

    for decade, docs in corpus.items():
        out[decade] = [PATTERN.sub(repl_fn, doc) for doc in docs]

    return out

def init_spacy() -> English:
    from spacy.util import compile_infix_regex
    
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    # remove hyphen from infix patterns so "ship-to-ship" stays one token
    infixes = list(nlp.Defaults.infixes)
    infixes = [x for x in infixes if "-" not in x]  # blunt but works well in practice
    infix_re = compile_infix_regex(infixes)
    nlp.tokenizer.infix_finditer = infix_re.finditer
    
    return nlp

def spacy_docs(corpus:list[str], nlp:English) -> list[Doc]:
    docs = list()
    for item in corpus:
        docs.append(nlp(item))
    return docs

def get_lemmas_and_non_stop_from_nlp(nlp_docs:list[Doc]) -> list[list[str]]:
    tokens = list()
    for doc in nlp_docs:
        lemmas_non_stop = list()
        for token in doc:
            if not token.is_stop:
                lemmas_non_stop.append(token.lemma_)
        tokens.append(lemmas_non_stop)
    return tokens

def create_per_decade_index(corpus_per_decade_lemmatized:dict[int,list[str]]) -> dict[int,tuple[int,int]]:
    index = {}
    start = 0
    
    for k, v in corpus_per_decade_lemmatized.items():
        end = start + len(v)
        index[k] = (start, end)
        start = end
    return index

def split_corpus_by_decade(global_model_corpus, index):
    return {
        decade: global_model_corpus[start:end]
        for decade, (start, end) in index.items()
    }