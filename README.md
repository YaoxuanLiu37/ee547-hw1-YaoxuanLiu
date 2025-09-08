# EE547 HW1 

Author: **Yaoxuan Liu**  
Email: **yaoxuanl@usc.edu**


## Notes on Implementation

In **Problem 3**, the pipeline consists of three services:

- **Fetcher**: downloads raw HTML pages from the given URLs.  
- **Processor**: cleans and processes the HTML into structured JSON.  
- **Analyzer**: performs corpus-level statistics and writes the final report.

### Modification in `fetch.py`

To ensure the shared volume has the expected folder structure, I added one extra line in **`fetch.py`**:

```python
os.makedirs("/shared/input", exist_ok=True)
```

This guarantees that `/shared/input/` exists before the pipeline writes or copies `urls.txt`.  
Without this line, the container could not find `/shared/input` when injecting URLs, causing the pipeline to fail.

