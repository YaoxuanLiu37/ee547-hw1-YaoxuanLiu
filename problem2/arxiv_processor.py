import sys, os, json, re, time
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone

API = "http://export.arxiv.org/api/query"
UA = "EE547-HW1-ArXivProcessor/1.0"
TIMEOUT = 10
MAX_ALLOWED = 100
STOPWORDS = {'the','a','an','and','or','but','in','on','at','to','for','of','with','by','from','up','about','into','through','during','is','are','was','were','be','been','being','have','has','had','do','does','did','will','would','could','should','may','might','can','this','that','these','those','i','you','he','she','it','we','they','what','which','who','when','where','why','how','all','each','every','both','few','more','most','other','some','such','as','also','very','too','only','so','than','not'}
WORD_RE = re.compile(r"[A-Za-z0-9]+")
RAWWORD_RE = re.compile(r"[A-Za-z0-9\-]+")
HYPHEN_RE = re.compile(r"\b[\w]+(?:-[\w]+)+\b")
SENT_SPLIT_RE = re.compile(r"[.!?]+")

def iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")

def log_line(log_path, msg):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{iso_now()} {msg}\n")

def http_get_with_retry(url, log_path):
    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            req = Request(url, headers={"User-Agent": UA})
            with urlopen(req, timeout=TIMEOUT) as resp:
                return resp.read()
        except HTTPError as e:
            if e.code == 429 and attempts < 3:
                log_line(log_path, f"[WARN] HTTP 429 rate limited, retry in 3s (attempt {attempts})")
                time.sleep(3)
                continue
            log_line(log_path, f"[ERROR] HTTPError {e.code}: {e.reason}")
            raise
        except (URLError, TimeoutError, OSError) as e:
            log_line(log_path, f"[ERROR] Network error: {e}")
            raise

def parse_feed(xml_bytes, log_path):
    try:
        ns = {"atom":"http://www.w3.org/2005/Atom","arxiv":"http://arxiv.org/schemas/atom"}
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        log_line(log_path, f"[ERROR] Invalid XML: {e}")
        return []
    entries = []
    for e in root.findall("atom:entry", ns):
        try:
            rid_full = (e.findtext("atom:id", default="", namespaces=ns) or "").strip()
            rid = rid_full.rsplit("/", 1)[-1] if rid_full else ""
            title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
            summary = (e.findtext("atom:summary", default="", namespaces=ns) or "").strip()
            published = (e.findtext("atom:published", default="", namespaces=ns) or "").strip()
            updated = (e.findtext("atom:updated", default="", namespaces=ns) or "").strip()
            authors = []
            for a in e.findall("atom:author", ns):
                name = a.findtext("atom:name", default="", namespaces=ns)
                if name:
                    authors.append(name.strip())
            categories = []
            for c in e.findall("atom:category", ns):
                term = c.attrib.get("term")
                if term:
                    categories.append(term)
            entries.append({
                "arxiv_id": rid,
                "title": title,
                "authors": authors,
                "abstract": summary,
                "categories": categories,
                "published": published,
                "updated": updated
            })
        except Exception as ex:
            log_line(log_path, f"[WARN] Entry parse error: {ex}")
            continue
    return entries

def abstract_stats(text):
    t = text or ""
    tokens = WORD_RE.findall(t)
    lower = [w.lower() for w in tokens]
    total_words = len(tokens)
    unique_words = len(set(lower))
    kept = [w for w in lower if w not in STOPWORDS]
    top20 = [{"word": w, "count": c} for w, c in Counter(kept).most_common(20)]
    avg_word_length = round(sum(len(w) for w in tokens)/total_words, 3) if total_words else 0.0
    sentences = [s for s in SENT_SPLIT_RE.split(t) if s.strip()]
    total_sentences = len(sentences)
    sent_word_counts = [len(WORD_RE.findall(s)) for s in sentences]
    avg_words_per_sentence = round(sum(sent_word_counts)/total_sentences, 3) if total_sentences else 0.0
    longest_sentence = max(sent_word_counts) if sent_word_counts else 0
    shortest_sentence = min(sent_word_counts) if sent_word_counts else 0
    raw_words = RAWWORD_RE.findall(t)
    uppercase_terms = sorted({w for w in raw_words if any(ch.isupper() for ch in w)})
    numeric_terms = sorted({w for w in raw_words if any(ch.isdigit() for ch in w)})
    hyphenated_terms = sorted({w for w in HYPHEN_RE.findall(t)})
    return {
        "total_words": total_words,
        "unique_words": unique_words,
        "top20": top20,
        "avg_word_length": avg_word_length,
        "total_sentences": total_sentences,
        "avg_words_per_sentence": avg_words_per_sentence,
        "longest_sentence": longest_sentence,
        "shortest_sentence": shortest_sentence,
        "uppercase_terms": uppercase_terms,
        "numeric_terms": numeric_terms,
        "hyphenated_terms": hyphenated_terms
    }

def main():
    if len(sys.argv) != 4:
        print('Usage: python arxiv_processor.py "<search_query>" <max_results 1-100> <output_dir>')
        sys.exit(1)
    query = sys.argv[1].strip()
    try:
        max_results = int(sys.argv[2])
    except ValueError:
        print("max_results must be an integer between 1 and 100")
        sys.exit(1)
    if not (1 <= max_results <= MAX_ALLOWED):
        print("max_results must be an integer between 1 and 100")
        sys.exit(1)
    out_dir = os.path.abspath(os.path.expanduser(sys.argv[3]))
    os.makedirs(out_dir, exist_ok=True)
    log_path = os.path.join(out_dir, "processing.log")
    start_ts = time.perf_counter()
    log_line(log_path, f"[INFO] Starting ArXiv query: {query}")
    params = {"search_query": query, "start": 0, "max_results": max_results}
    url = f"{API}?{urlencode(params)}"
    try:
        xml_bytes = http_get_with_retry(url, log_path)
    except Exception as e:
        log_line(log_path, f"[ERROR] Network failure, exiting: {e}")
        print("Network error; see processing.log")
        sys.exit(1)
    entries = parse_feed(xml_bytes, log_path)
    log_line(log_path, f"[INFO] Fetched {len(entries)} results from ArXiv API")
    valid = []
    for ent in entries:
        if not ent.get("arxiv_id") or not ent.get("title") or not ent.get("abstract"):
            log_line(log_path, f"[WARN] Missing required fields; skipped id={ent.get('arxiv_id','')}")
            continue
        st = abstract_stats(ent["abstract"])
        ent_out = {
            "arxiv_id": ent["arxiv_id"],
            "title": ent["title"],
            "authors": ent.get("authors",[]),
            "abstract": ent["abstract"],
            "categories": ent.get("categories",[]),
            "published": ent.get("published",""),
            "updated": ent.get("updated",""),
            "abstract_stats": {
                "total_words": st["total_words"],
                "unique_words": st["unique_words"],
                "total_sentences": st["total_sentences"],
                "avg_words_per_sentence": st["avg_words_per_sentence"],
                "avg_word_length": st["avg_word_length"]
            }
        }
        log_line(log_path, f"[INFO] Processing paper: {ent['arxiv_id']}")
        valid.append((ent_out, st))
    papers = [p for p,_ in valid]
    papers_path = os.path.join(out_dir, "papers.json")
    with open(papers_path, "w", encoding="utf-8") as f:
        json.dump(papers, f, indent=2, ensure_ascii=False)
    total_words = sum(st["total_words"] for _,st in valid)
    all_tokens_lower = []
    doc_presence = defaultdict(int)
    for _,st in valid:
        seen = set()
        for w,c in st["top20"]:
            pass
        for w in WORD_RE.findall((_["abstract"] if False else "")):
            pass
    global_counter = Counter()
    doc_counter = Counter()
    for ent_out, st in valid:
        toks = [w.lower() for w in WORD_RE.findall(ent_out["abstract"])]
        toks = [w for w in toks if w not in STOPWORDS]
        counts = Counter(toks)
        global_counter.update(counts)
        for w in counts.keys():
            doc_counter[w] += 1
    top_50 = [{"word": w, "frequency": c, "documents": doc_counter[w]} for w,c in global_counter.most_common(50)]
    uppercase_terms = sorted({w for _,st in valid for w in st["uppercase_terms"]})
    numeric_terms = sorted({w for _,st in valid for w in st["numeric_terms"]})
    hyphenated_terms = sorted({w for _,st in valid for w in st["hyphenated_terms"]})
    unique_global = set()
    abstract_lengths = []
    for ent_out, st in valid:
        toks = [w.lower() for w in WORD_RE.findall(ent_out["abstract"])]
        unique_global.update(toks)
        abstract_lengths.append(st["total_words"])
    avg_abstract_length = round(sum(abstract_lengths)/len(abstract_lengths), 3) if abstract_lengths else 0.0
    longest_abs = max(abstract_lengths) if abstract_lengths else 0
    shortest_abs = min(abstract_lengths) if abstract_lengths else 0
    cat_dist = Counter()
    for ent_out, _ in valid:
        for c in ent_out.get("categories",[]):
            cat_dist[c] += 1
    corpus = {
        "query": query,
        "papers_processed": len(valid),
        "processing_timestamp": iso_now(),
        "corpus_stats": {
            "total_abstracts": len(valid),
            "total_words": total_words,
            "unique_words_global": len(unique_global),
            "avg_abstract_length": avg_abstract_length,
            "longest_abstract_words": longest_abs,
            "shortest_abstract_words": shortest_abs
        },
        "top_50_words": top_50,
        "technical_terms": {
            "uppercase_terms": uppercase_terms,
            "numeric_terms": numeric_terms,
            "hyphenated_terms": hyphenated_terms
        },
        "category_distribution": dict(cat_dist)
    }
    corpus_path = os.path.join(out_dir, "corpus_analysis.json")
    with open(corpus_path, "w", encoding="utf-8") as f:
        json.dump(corpus, f, indent=2, ensure_ascii=False)
    elapsed = time.perf_counter() - start_ts
    log_line(log_path, f"[INFO] Completed processing: {len(valid)} papers in {elapsed:.2f} seconds")
    print(f"[OK] Wrote: {papers_path}, {corpus_path}, {log_path}")

if __name__ == "__main__":
    main()
