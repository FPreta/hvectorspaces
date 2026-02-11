def sanity_check(
    model,
    queries: list[str],
    topn: int = 20,
) -> None:
    
    wv = model.wv
    
    for q in queries:
        if q in wv:
            count = wv.get_vecattr(q, "count")
            print(f"{q} (count={count}) →")
            print(wv.most_similar(q, topn=topn))
            print()
        else:
            print(f"{q} not in vocab")