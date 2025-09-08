#!/usr/bin/env python3
import os, json, time, glob, re, itertools, collections
from datetime import datetime, timezone

def iso_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def tokenize(text):
    return re.findall(r"[A-Za-z0-9]+", text or "")

def jaccard_similarity(doc1_words, doc2_words):
    set1, set2 = set(doc1_words), set(doc2_words)
    inter = set1.intersection(set2)
    union = set1.union(set2)
    return len(inter) / len(union) if union else 0.0

def ngrams(words, n):
    return [" ".join(words[i:i+n]) for i in range(len(words)-n+1)] if n>0 else []

def main():
    print(f"[{iso_utc()}] Analyzer starting", flush=True)

    status_dir = "/shared/status"
    proc_dir   = "/shared/processed"
    out_dir    = "/shared/analysis"
    os.makedirs(out_dir, exist_ok=True)

    marker = os.path.join(status_dir, "process_complete.json")
    while not os.path.exists(marker):
        print(f"[{iso_utc()}] Waiting for {marker} ...", flush=True)
        time.sleep(2)

    files = sorted(glob.glob(os.path.join(proc_dir, "page_*.json")))
    print(f"[{iso_utc()}] Found {len(files)} processed documents", flush=True)

    docs = []
    total_words = 0
    total_word_len = 0
    total_sentences = 0

    global_word_counter = collections.Counter()
    bigram_counter = collections.Counter()
    trigram_counter = collections.Counter()

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        text = obj.get("text","")
        tokens = [w.lower() for w in tokenize(text)]
        global_word_counter.update(tokens)
        bigram_counter.update(ngrams(tokens, 2))
        trigram_counter.update(ngrams(tokens, 3))

        stats = obj.get("statistics", {})
        wc = int(stats.get("word_count", len(tokens)))
        sc = int(stats.get("sentence_count", 0))
        total_words += wc
        total_sentences += sc if sc>=0 else 0
        total_word_len += sum(len(w) for w in tokens)

        docs.append({
            "name": os.path.basename(path),
            "tokens": tokens
        })

    top_100 = global_word_counter.most_common(100)
    report_top100 = [
        {"word": w, "count": c, "frequency": (c/total_words if total_words else 0.0)}
        for w, c in top_100
    ]

    sim_list = []
    for d1, d2 in itertools.combinations(docs, 2):
        sim = jaccard_similarity(d1["tokens"], d2["tokens"])
        sim_list.append({"doc1": d1["name"], "doc2": d2["name"], "similarity": round(sim, 6)})

    top_bigrams = [{"bigram": bg, "count": c} for bg, c in bigram_counter.most_common(100)]
    top_trigrams = [{"trigram": tg, "count": c} for tg, c in trigram_counter.most_common(100)]

    avg_sentence_length = (total_words/total_sentences) if total_sentences else 0.0
    avg_word_length = (total_word_len/total_words) if total_words else 0.0
    complexity_score = avg_sentence_length * avg_word_length

    report = {
        "processing_timestamp": iso_utc(),
        "documents_processed": len(docs),
        "total_words": total_words,
        "unique_words": len(global_word_counter),
        "top_100_words": [
            {"word": r["word"], "count": r["count"], "frequency": round(r["frequency"], 6)}
            for r in report_top100
        ],
        "document_similarity": sim_list,
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "readability": {
            "avg_sentence_length": round(avg_sentence_length, 6),
            "avg_word_length": round(avg_word_length, 6),
            "complexity_score": round(complexity_score, 6)
        }
    }

    out_path = os.path.join(out_dir, "final_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[{iso_utc()}] Analyzer complete -> {out_path}", flush=True)

    try:
        for _ in range(180):
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
