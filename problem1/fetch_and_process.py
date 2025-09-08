import sys
import os
import json
import time
import re
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# -------- Config --------
USER_AGENT = "EE547-HW1-HTTPFetcher/1.0"
TIMEOUT_SEC = 10
WORD_RE = re.compile(r"[A-Za-z0-9]+")  # alphanumeric sequences only

# -------- Helpers --------
def now_utc_iso() -> str:
    # ISO-8601 UTC with 'Z' suffix, millisecond precision
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def is_text_content(content_type: str) -> bool:
    return (content_type or "").lower().find("text") != -1

def count_words(text: str) -> int:
    if not text:
        return 0
    return len(WORD_RE.findall(text))

def ensure_dir(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

# -------- Core --------
def process_url(url: str):

    t0 = time.perf_counter()
    timestamp = now_utc_iso()
    req = Request(url, headers={"User-Agent": USER_AGENT})

    try:
        # Successful 2xx/3xx
        with urlopen(req, timeout=TIMEOUT_SEC) as resp:
            body = resp.read()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            status_code = getattr(resp, "status", None)
            content_type = resp.headers.get("Content-Type", "")

            if is_text_content(content_type):
                # Try best-effort UTF-8 decoding (ignore errors); spec doesn't constrain encoding
                try:
                    text = body.decode("utf-8", errors="ignore")
                    wc = count_words(text)
                except Exception:
                    wc = 0
            else:
                wc = None  # non-text => null in JSON

            return {
                "url": url,
                "status_code": int(status_code) if status_code is not None else None,
                "response_time_ms": float(round(elapsed_ms, 3)),
                "content_length": int(len(body)),
                "word_count": wc if wc is not None else None,
                "timestamp": timestamp,
                "error": None
            }, False, None

    except HTTPError as e:
        # HTTPError still gives us a response (e.code, e.headers, e.read())
        body = e.read() or b""
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        content_type = getattr(e, "headers", {}).get("Content-Type", "") if hasattr(e, "headers") else ""

        if is_text_content(content_type):
            try:
                text = body.decode("utf-8", errors="ignore")
                wc = count_words(text)
            except Exception:
                wc = 0
        else:
            wc = None

        rec = {
            "url": url,
            "status_code": int(getattr(e, "code", 0)) if getattr(e, "code", None) is not None else None,
            "response_time_ms": float(round(elapsed_ms, 3)),
            "content_length": int(len(body)),
            "word_count": wc if wc is not None else None,
            "timestamp": timestamp,
            "error": None  # HTTP error is still a received response, not a transport failure
        }
        return rec, False, None

    except (URLError, TimeoutError, ValueError, OSError) as e:
        # True request failure: record error and continue
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        rec = {
            "url": url,
            "status_code": None,
            "response_time_ms": float(round(elapsed_ms, 3)),
            "content_length": 0,
            "word_count": None,
            "timestamp": timestamp,
            "error": f"{type(e).__name__}: {e}"
        }
        err_line = f"{timestamp} {url}: {rec['error']}"
        return rec, True, err_line

def main():
    # ---- Args ----
    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_urls.txt> <output_dir>")
        sys.exit(1)
    input_file = os.path.abspath(os.path.expanduser(sys.argv[1]))
    output_dir = os.path.abspath(os.path.expanduser(sys.argv[2]))

    if not os.path.isfile(input_file):
        print(f"[ERROR] Input file not found: {input_file}")
        sys.exit(2)
    ensure_dir(output_dir)

    # ---- Read URLs ----
    with open(input_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    # ---- Processing ----
    processing_start = now_utc_iso()
    responses = []
    errors = []

    for url in urls:
        rec, had_error, err_line = process_url(url)
        responses.append(rec)
        if had_error and err_line:
            errors.append(err_line)

    processing_end = now_utc_iso()

    # ---- Write responses.json ----
    responses_path = os.path.join(output_dir, "responses.json")
    with open(responses_path, "w", encoding="utf-8") as f:
        json.dump(responses, f, indent=2, ensure_ascii=False)

    # ---- summary.json ----
    total_urls = len(responses)
    successful_requests = sum(1 for r in responses if r["error"] is None)
    failed_requests = total_urls - successful_requests
    avg_ms = round(sum(r["response_time_ms"] for r in responses) / total_urls, 3) if total_urls else 0.0
    total_bytes = sum(int(r["content_length"]) for r in responses)

    status_code_distribution = {}
    for r in responses:
        code = r["status_code"]
        if code is not None:
            key = str(int(code))
            status_code_distribution[key] = status_code_distribution.get(key, 0) + 1

    summary = {
        "total_urls": total_urls,
        "successful_requests": successful_requests,
        "failed_requests": failed_requests,
        "average_response_time_ms": float(avg_ms),
        "total_bytes_downloaded": int(total_bytes),
        "status_code_distribution": status_code_distribution,
        "processing_start": processing_start,
        "processing_end": processing_end
    }

    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # ---- errors.log ----
    errors_path = os.path.join(output_dir, "errors.log")
    with open(errors_path, "w", encoding="utf-8") as f:
        for line in errors:
            f.write(line + "\n")

    print(f"[OK] Wrote: {responses_path}, {summary_path}, {errors_path}")

if __name__ == "__main__":
    main()
