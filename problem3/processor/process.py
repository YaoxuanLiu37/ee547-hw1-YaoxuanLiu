#!/usr/bin/env python3
import os, re, json, time, glob
from datetime import datetime, timezone

def iso_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def strip_html(html_content):
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', html_content)
    text = re.sub(r'\s+', ' ', text).strip()
    return text, links, images

def word_stats(text):
    tokens = re.findall(r'[A-Za-z0-9]+', text)
    wc = len(tokens)
    avg_wlen = round(sum(len(w) for w in tokens)/wc, 3) if wc else 0.0
    return wc, avg_wlen

def sentence_count(text):
    if not text:
        return 0
    parts = re.split(r'[.!?]+', text)
    return len([p for p in parts if p.strip()])

def paragraph_count_from_html(html):
    c = 0
    for pat in (r'<\s*p\b', r'<\s*br\b', r'</\s*p\s*>', r'<\s*div\b', r'<\s*li\b'):
        c += len(re.findall(pat, html, flags=re.IGNORECASE))
    if c == 0:
        t, _, _ = strip_html(html)
        return 1 if t else 0
    return c

def main():
    print(f"[{iso_utc()}] Processor starting", flush=True)
    status_dir = "/shared/status"
    raw_dir = "/shared/raw"
    out_dir = "/shared/processed"
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(status_dir, exist_ok=True)
    fetch_done = os.path.join(status_dir, "fetch_complete.json")
    while not os.path.exists(fetch_done):
        print(f"[{iso_utc()}] Waiting for {fetch_done} ...", flush=True)
        time.sleep(2)
    html_files = sorted(glob.glob(os.path.join(raw_dir, "page_*.html")))
    print(f"[{iso_utc()}] Found {len(html_files)} HTML files", flush=True)
    processed = []
    for path in html_files:
        fname = os.path.basename(path)
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            text, links, images = strip_html(html)
            wc, avg_wlen = word_stats(text)
            sc = sentence_count(text)
            pc = paragraph_count_from_html(html)
            out_obj = {
                "source_file": fname,
                "text": text,
                "statistics": {
                    "word_count": wc,
                    "sentence_count": sc,
                    "paragraph_count": pc,
                    "avg_word_length": avg_wlen
                },
                "links": links,
                "images": images,
                "processed_at": iso_utc()
            }
            base = os.path.splitext(fname)[0] + ".json"
            out_path = os.path.join(out_dir, base)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(out_obj, f, ensure_ascii=False, indent=2)
            processed.append({"input": fname, "output": base, "status": "success"})
            print(f"[{iso_utc()}] Processed {fname} -> {base}", flush=True)
        except Exception as e:
            processed.append({"input": fname, "output": None, "status": "failed", "error": str(e)})
            print(f"[{iso_utc()}] ERROR processing {fname}: {e}", flush=True)
        time.sleep(0.2)
    summary = {
        "timestamp": iso_utc(),
        "files_seen": len(html_files),
        "processed_success": sum(1 for x in processed if x["status"] == "success"),
        "processed_failed": sum(1 for x in processed if x["status"] == "failed"),
        "results": processed
    }
    with open(os.path.join(status_dir, "process_complete.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"[{iso_utc()}] Processor complete", flush=True)

if __name__ == "__main__":
    main()
