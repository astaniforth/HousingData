#!/usr/bin/env python3
"""
Script to update the notebook to use link text as PDF filenames.
This reads the notebook, makes the necessary changes, and writes it back.
"""

import json
import re

# Read the notebook
with open('test_ceqr_api.ipynb', 'r', encoding='utf-8') as f:
    notebook = json.load(f)

# Find the cell with scrape_detail_page function
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'def scrape_detail_page' in ''.join(cell['source']):
        source = ''.join(cell['source'])
        
        # Update the docstring
        source = source.replace(
            "- 'pdf_links': List of PDF URLs found on the page",
            "- 'pdf_links': List of dicts with 'url' and 'filename' keys"
        )
        
        # Update the result dict comment
        source = source.replace(
            "'pdf_links': [],",
            "'pdf_links': [],  # List of dicts: [{'url': '...', 'filename': '...'}, ...]"
        )
        
        # Add sanitize_filename function before the link loop
        if 'def sanitize_filename' not in source:
            # Find where to insert it (before the link loop)
            insert_pos = source.find('for link in soup.find_all(\'a\', href=True):')
            if insert_pos > 0:
                sanitize_func = '''
        # Helper function to sanitize filename
        def sanitize_filename(text):
            """Clean text to make it a valid filename."""
            import re
            # Remove or replace invalid filename characters
            text = re.sub(r'[<>:"/\\\\|?*]', '_', text)
            # Remove leading/trailing spaces and dots
            text = text.strip('. ')
            # Limit length
            if len(text) > 200:
                text = text[:200]
            # Ensure it ends with .pdf if it doesn't already
            if not text.lower().endswith('.pdf'):
                text = text + '.pdf'
            return text
        
'''
                source = source[:insert_pos] + sanitize_func + source[insert_pos:]
        
        # Update link_text to preserve case
        source = source.replace(
            "link_text = link.get_text(strip=True).lower()",
            "link_text = link.get_text(strip=True)  # Keep original case for filename\n            link_text_lower = link_text.lower()"
        )
        
        # Update references to link_text to use link_text_lower for checks
        source = re.sub(
            r"('pdf' in link_text|'kb' in link_text|'mb' in link_text|link_text\.endswith)",
            r"\1_lower",
            source
        )
        
        # Update the append to store dict instead of URL
        source = re.sub(
            r"normalized_url = normalize_url\(href, detail_url\)\s+pdf_links\.append\(normalized_url\)",
            '''normalized_url = normalize_url(href, detail_url)
                
                # Use link text as filename, or generate one if empty
                if link_text:
                    filename = sanitize_filename(link_text)
                else:
                    # Fallback: use URL hash if no link text
                    import hashlib
                    url_hash = hashlib.md5(normalized_url.encode()).hexdigest()[:12]
                    filename = f"ceqr_file_{url_hash}.pdf"
                
                pdf_links.append({
                    'url': normalized_url,
                    'filename': filename
                })''',
            source
        )
        
        # Update iframe handling
        source = re.sub(
            r"normalized_url = normalize_url\(src, detail_url\)\s+pdf_links\.append\(normalized_url\)",
            '''normalized_url = normalize_url(src, detail_url)
                # For iframes, use URL-based filename
                import hashlib
                url_hash = hashlib.md5(normalized_url.encode()).hexdigest()[:12]
                filename = f"ceqr_file_{url_hash}.pdf"
                pdf_links.append({
                    'url': normalized_url,
                    'filename': filename
                })''',
            source
        )
        
        # Update file_sections handling
        old_pattern = r"normalized_url = normalize_url\(href, detail_url\)\s+if normalized_url not in pdf_links:\s+pdf_links\.append\(normalized_url\)"
        new_code = '''normalized_url = normalize_url(href, detail_url)
                    if normalized_url not in seen_urls:
                        seen_urls.add(normalized_url)
                        link_text = link.get_text(strip=True)
                        if link_text:
                            filename = sanitize_filename(link_text)
                        else:
                            import hashlib
                            url_hash = hashlib.md5(normalized_url.encode()).hexdigest()[:12]
                            filename = f"ceqr_file_{url_hash}.pdf"
                        pdf_links.append({
                            'url': normalized_url,
                            'filename': filename
                        })'''
        source = re.sub(old_pattern, new_code, source, flags=re.MULTILINE)
        
        # Update deduplication to work with dicts
        source = re.sub(
            r"# Remove duplicates while preserving order\s+seen = set\(\)\s+unique_pdf_links = \[\]\s+for link in pdf_links:\s+if link not in seen:\s+seen\.add\(link\)\s+unique_pdf_links\.append\(link\)",
            '''# Remove duplicates by URL while preserving order
        seen = set()
        unique_pdf_links = []
        for pdf_info in pdf_links:
            if pdf_info['url'] not in seen:
                seen.add(pdf_info['url'])
                unique_pdf_links.append(pdf_info)''',
            source,
            flags=re.MULTILINE
        )
        
        # Add seen_urls initialization before file_sections
        if 'seen_urls = set()' not in source and 'file_sections = soup.find_all' in source:
            source = source.replace(
                'file_sections = soup.find_all',
                'seen_urls = set()\n        file_sections = soup.find_all'
            )
        
        # Update cell source
        notebook['cells'][i]['source'] = source.split('\n')
        print(f"Updated scrape_detail_page function in cell {i}")
        break

# Update scrape_all_detail_pages to store as JSON
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'def scrape_all_detail_pages' in ''.join(cell['source']):
        source = ''.join(cell['source'])
        
        # Update docstring
        source = source.replace(
            "- 'pdf_links': List of PDF URLs (as string, comma-separated)",
            "- 'pdf_links': JSON string of list of dicts with 'url' and 'filename'"
        )
        
        # Update storage to use JSON
        source = source.replace(
            "df.at[idx, 'pdf_links'] = ', '.join(result['pdf_links'])",
            "import json\n        df.at[idx, 'pdf_links'] = json.dumps(result['pdf_links'])"
        )
        
        notebook['cells'][i]['source'] = source.split('\n')
        print(f"Updated scrape_all_detail_pages function in cell {i}")
        break

# Update download_all_pdfs to parse JSON and use filenames
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'def download_all_pdfs' in ''.join(cell['source']):
        source = ''.join(cell['source'])
        
        # Update to parse JSON and extract URLs and filenames
        old_pattern = r"pdf_links_str = row\[pdf_links_column\]\s+if not pdf_links_str or pd\.isna\(pdf_links_str\) or pdf_links_str == '':\s+continue\s+# Parse comma-separated links\s+pdf_urls = \[url\.strip\(\) for url in str\(pdf_links_str\)\.split\(','\) if url\.strip\(\)\]"
        new_code = '''pdf_links_str = row[pdf_links_column]
        
        if not pdf_links_str or pd.isna(pdf_links_str) or pdf_links_str == '':
            continue
        
        # Parse JSON string to get list of dicts with 'url' and 'filename'
        try:
            import json
            pdf_list = json.loads(pdf_links_str)
        except:
            # Fallback: treat as comma-separated URLs (old format)
            pdf_list = [{'url': url.strip(), 'filename': None} for url in str(pdf_links_str).split(',') if url.strip()]'''
        
        source = re.sub(old_pattern, new_code, source, flags=re.MULTILINE)
        
        # Update the download loop
        old_loop = r"for pdf_url in pdf_urls:"
        new_loop = '''for pdf_info in pdf_list:
            pdf_url = pdf_info['url'] if isinstance(pdf_info, dict) else pdf_info
            pdf_filename = pdf_info.get('filename') if isinstance(pdf_info, dict) else None'''
        
        source = source.replace(old_loop, new_loop)
        
        # Update download_pdf call to include filename
        source = source.replace(
            "result = download_pdf(pdf_url, output_dir, session)",
            "result = download_pdf(pdf_url, output_dir, session, filename=pdf_filename)"
        )
        
        notebook['cells'][i]['source'] = source.split('\n')
        print(f"Updated download_all_pdfs function in cell {i}")
        break

# Write the updated notebook
with open('test_ceqr_api.ipynb', 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

print("Notebook updated successfully!")

