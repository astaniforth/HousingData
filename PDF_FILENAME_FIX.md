# PDF Filename Fix - Manual Instructions

To make PDFs save with filenames matching the visible link text, make these changes:

## 1. In `scrape_detail_page()` function (cell 6):

### Change this line:
```python
link_text = link.get_text(strip=True).lower()
```

### To:
```python
link_text_original = link.get_text(strip=True)  # Keep original for filename
link_text = link_text_original.lower()  # Use lowercase for checks
```

### Change this block:
```python
if is_pdf or (has_pdf_indicator and ('handler' in href.lower() or 'file=' in href.lower() or 'ashx' in href.lower())):
    normalized_url = normalize_url(href, detail_url)
    pdf_links.append(normalized_url)
```

### To:
```python
if is_pdf or (has_pdf_indicator and ('handler' in href.lower() or 'file=' in href.lower() or 'ashx' in href.lower())):
    normalized_url = normalize_url(href, detail_url)
    
    # Use link text as filename, sanitize it
    import re
    if link_text_original:
        filename = re.sub(r'[<>:"/\\|?*]', '_', link_text_original)
        filename = filename.strip('. ')
        if len(filename) > 200:
            filename = filename[:200]
        if not filename.lower().endswith('.pdf'):
            filename = filename + '.pdf'
    else:
        import hashlib
        filename = f"ceqr_file_{hashlib.md5(normalized_url.encode()).hexdigest()[:12]}.pdf"
    
    pdf_links.append({
        'url': normalized_url,
        'filename': filename
    })
```

### Change deduplication:
```python
# Remove duplicates while preserving order
seen = set()
unique_pdf_links = []
for link in pdf_links:
    if link not in seen:
        seen.add(link)
        unique_pdf_links.append(link)
```

### To:
```python
# Remove duplicates by URL while preserving order
seen = set()
unique_pdf_links = []
for pdf_info in pdf_links:
    if pdf_info['url'] not in seen:
        seen.add(pdf_info['url'])
        unique_pdf_links.append(pdf_info)
```

## 2. In `scrape_all_detail_pages()` function (cell 6):

### Change:
```python
df.at[idx, 'pdf_links'] = ', '.join(result['pdf_links'])
```

### To:
```python
import json
df.at[idx, 'pdf_links'] = json.dumps(result['pdf_links'])
```

## 3. In `download_all_pdfs()` function (cell 8):

### Change:
```python
# Parse comma-separated links
pdf_urls = [url.strip() for url in str(pdf_links_str).split(',') if url.strip()]

for pdf_url in pdf_urls:
    stats['total_pdfs'] += 1
    
    result = download_pdf(pdf_url, output_dir, session)
```

### To:
```python
# Parse JSON string to get list of dicts with 'url' and 'filename'
try:
    import json
    pdf_list = json.loads(pdf_links_str)
except:
    # Fallback: treat as comma-separated URLs (old format)
    pdf_list = [{'url': url.strip(), 'filename': None} for url in str(pdf_links_str).split(',') if url.strip()]

for pdf_info in pdf_list:
    stats['total_pdfs'] += 1
    pdf_url = pdf_info['url'] if isinstance(pdf_info, dict) else pdf_info
    pdf_filename = pdf_info.get('filename') if isinstance(pdf_info, dict) else None
    
    result = download_pdf(pdf_url, output_dir, session, filename=pdf_filename)
```

## Summary

The key changes are:
1. Capture original link text (not just lowercase)
2. Store PDF info as dicts with 'url' and 'filename' keys
3. Store in DataFrame as JSON string
4. Parse JSON when downloading and pass filename to download_pdf()

