#!/usr/bin/env python3
"""
Fix PDF filenames to use link text - properly edits Jupyter notebook using nbformat.
"""

import nbformat
import re

# Read the notebook
with open('test_ceqr_api.ipynb', 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# Find and update scrape_detail_page function
for cell in nb.cells:
    if cell.cell_type == 'code' and 'def scrape_detail_page' in cell.source:
        source = cell.source
        
        # Change link_text capture
        source = source.replace(
            "link_text = link.get_text(strip=True).lower()",
            "link_text_original = link.get_text(strip=True)  # Keep original for filename\n            link_text = link_text_original.lower()  # Use lowercase for checks"
        )
        
        # Update the append block
        old_append = """if is_pdf or (has_pdf_indicator and ('handler' in href.lower() or 'file=' in href.lower() or 'ashx' in href.lower())):
                normalized_url = normalize_url(href, detail_url)
                pdf_links.append(normalized_url)"""
        
        new_append = """if is_pdf or (has_pdf_indicator and ('handler' in href.lower() or 'file=' in href.lower() or 'ashx' in href.lower())):
                normalized_url = normalize_url(href, detail_url)
                
                # Use link text as filename, sanitize it
                import re
                if link_text_original:
                    filename = re.sub(r'[<>:"/\\\\|?*]', '_', link_text_original)
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
                })"""
        
        source = source.replace(old_append, new_append)
        
        # Update deduplication
        source = source.replace(
            """# Remove duplicates while preserving order
        seen = set()
        unique_pdf_links = []
        for link in pdf_links:
            if link not in seen:
                seen.add(link)
                unique_pdf_links.append(link)""",
            """# Remove duplicates by URL while preserving order
        seen = set()
        unique_pdf_links = []
        for pdf_info in pdf_links:
            if pdf_info['url'] not in seen:
                seen.add(pdf_info['url'])
                unique_pdf_links.append(pdf_info)"""
        )
        
        cell.source = source
        print("✅ Updated scrape_detail_page function")

# Find and update scrape_all_detail_pages function
for cell in nb.cells:
    if cell.cell_type == 'code' and 'def scrape_all_detail_pages' in cell.source:
        source = cell.source
        
        # Update storage
        source = source.replace(
            "df.at[idx, 'pdf_links'] = ', '.join(result['pdf_links'])",
            "import json\n        df.at[idx, 'pdf_links'] = json.dumps(result['pdf_links'])"
        )
        
        cell.source = source
        print("✅ Updated scrape_all_detail_pages function")

# Find and update download_all_pdfs function
for cell in nb.cells:
    if cell.cell_type == 'code' and 'def download_all_pdfs' in cell.source:
        source = cell.source
        
        # Update parsing and download loop
        old_parse = """# Parse comma-separated links
        pdf_urls = [url.strip() for url in str(pdf_links_str).split(',') if url.strip()]
        
        for pdf_url in pdf_urls:
            stats['total_pdfs'] += 1
            
            result = download_pdf(pdf_url, output_dir, session)"""
        
        new_parse = """# Parse JSON string to get list of dicts with 'url' and 'filename'
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
            
            result = download_pdf(pdf_url, output_dir, session, filename=pdf_filename)"""
        
        source = source.replace(old_parse, new_parse)
        
        cell.source = source
        print("✅ Updated download_all_pdfs function")

# Write the updated notebook
with open('test_ceqr_api.ipynb', 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("\n✅ Notebook updated successfully!")

