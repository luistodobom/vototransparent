import os
import re
import time
import pypdf
import tabula
import hashlib
import requests
import pandas as pd
import fitz  # PyMuPDF
from urllib.parse import urlparse, parse_qs

from config import (
    DOWNLOAD_TIMEOUT,
    SESSION_PDF_DIR,
    PROPOSAL_DOC_DIR,
    DATAFRAME_PATH,
    HTTP_RETRY_ATTEMPTS,
    HTTP_RETRY_BASE_DELAY,
    HTTP_RETRY_MAX_DELAY,
    HTTP_RETRY_MAX_TOTAL_TIME
)


def http_request_with_retry(url, headers=None, timeout=DOWNLOAD_TIMEOUT, stream=False):
    """
    Makes an HTTP request with exponential backoff retry logic.
    
    Args:
        url (str): The URL to request
        headers (dict): HTTP headers to include
        timeout (int): Request timeout in seconds
        stream (bool): Whether to stream the response
        
    Returns:
        tuple: (response, error_message) where response is the requests.Response object or None
    """
    start_time = time.time()
    
    for attempt in range(HTTP_RETRY_ATTEMPTS):
        try:
            print(f"Attempting HTTP request to {url} (attempt {attempt + 1}/{HTTP_RETRY_ATTEMPTS})")
            response = requests.get(url, headers=headers, timeout=timeout, stream=stream)
            response.raise_for_status()
            return response, None
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            elapsed_time = time.time() - start_time
            
            # Check if we've exceeded the maximum total time
            if elapsed_time >= HTTP_RETRY_MAX_TOTAL_TIME:
                print(f"Maximum total retry time ({HTTP_RETRY_MAX_TOTAL_TIME}s) exceeded for {url}")
                return None, f"Request timed out after {elapsed_time:.1f}s total (max {HTTP_RETRY_MAX_TOTAL_TIME}s): {e}"
            
            # Calculate exponential backoff delay
            delay = min(HTTP_RETRY_BASE_DELAY * (2 ** attempt), HTTP_RETRY_MAX_DELAY)
            
            # Check if delay would exceed remaining time budget
            remaining_time = HTTP_RETRY_MAX_TOTAL_TIME - elapsed_time
            if delay > remaining_time:
                delay = max(0, remaining_time - 1)  # Leave 1 second for the actual request
            
            print(f"Request failed (attempt {attempt + 1}/{HTTP_RETRY_ATTEMPTS}): {e}")
            
            # Don't sleep on the last attempt
            if attempt + 1 < HTTP_RETRY_ATTEMPTS and delay > 0:
                print(f"Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
            elif attempt + 1 == HTTP_RETRY_ATTEMPTS:
                return None, f"Request failed after {HTTP_RETRY_ATTEMPTS} attempts: {e}"
                
        except requests.exceptions.RequestException as e:
            # For non-retryable errors (like 404, 403, etc.), don't retry
            print(f"Non-retryable error for {url}: {e}")
            return None, f"Request failed with non-retryable error: {e}"
    
    return None, f"Request failed after {HTTP_RETRY_ATTEMPTS} attempts"


def init_directories():
    """Creates necessary data directories if they don't exist."""
    os.makedirs(SESSION_PDF_DIR, exist_ok=True)
    os.makedirs(PROPOSAL_DOC_DIR, exist_ok=True)
    print(f"Ensured directories exist: {SESSION_PDF_DIR}, {PROPOSAL_DOC_DIR}")


def load_or_initialize_dataframe(dataframe_path=None):
    """Loads the DataFrame from CSV if it exists, otherwise initializes an empty one."""
    df_path = dataframe_path if dataframe_path else DATAFRAME_PATH
    if os.path.exists(df_path):
        print(f"Loading existing DataFrame from {df_path}")
        try:
            df = pd.read_csv(df_path)
        except pd.errors.EmptyDataError:
            print(
                f"Warning: {DATAFRAME_PATH} is empty. Initializing a new DataFrame.")
            df = pd.DataFrame(columns=get_dataframe_columns())
        except Exception as e:
            print(
                f"Error loading DataFrame: {e}. Initializing a new DataFrame.")
            df = pd.DataFrame(columns=get_dataframe_columns())
    else:
        print("Initializing new DataFrame.")
        df = pd.DataFrame(columns=get_dataframe_columns())

    # Ensure all columns are present, add if missing (for schema evolution)
    expected_columns = get_dataframe_columns()
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA  # Use pd.NA for missing values
    df = df[expected_columns]  # Reorder columns to expected order

    # Convert object columns that might contain pd.NA to a nullable string type if appropriate,
    # or ensure they are handled correctly. For now, rely on pd.NA handling.
    return df


def get_dataframe_columns():
    """Returns the list of expected DataFrame columns."""
    return [
        'session_pdf_url', 'session_year', 'session_date', 'session_pdf_text_path', 'session_pdf_download_status',
        'proposal_name_from_session', 'proposal_gov_link', 'voting_details_json', 'session_parse_status',
        'proposal_authors_json', 'proposal_document_url', 'proposal_document_type',
        'proposal_document_local_path', 'proposal_doc_download_status', 'proposal_details_scrape_status',
        'proposal_summary_general', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact',
        'proposal_summary_colloquial', 'proposal_category', 'proposal_summarize_status',
        'proposal_approval_status', 'proposal_short_title', 'proposal_proposing_party',
        'overall_status', 'last_error_message', 'last_processed_timestamp'
    ]


def save_dataframe(df, dataframe_path=None):
    """Saves the DataFrame to CSV."""
    try:
        df_path = dataframe_path if dataframe_path else DATAFRAME_PATH
        df.to_csv(df_path, index=False)
        print(f"DataFrame saved to {df_path}")
    except Exception as e:
        print(f"Error saving DataFrame: {e}")


def download_file(url, destination_path, is_pdf=True):
    """Downloads a file from a URL to a destination path with retry logic."""
    # Check if file already exists
    if os.path.exists(destination_path):
        file_size = os.path.getsize(destination_path)
        if file_size > 0:  # File exists and is not empty
            print(f"File already exists and is non-empty: {destination_path} ({file_size} bytes)")
            return True, destination_path
        else:
            print(f"File exists but is empty, re-downloading: {destination_path}")
    
    print(f"Attempting to download: {url} to {destination_path}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response, error = http_request_with_retry(url, headers=headers, timeout=DOWNLOAD_TIMEOUT, stream=True)
    
    if error:
        print(f"Error downloading {url}: {error}")
        return False, error
    
    try:
        # Check content type for PDFs if expected
        if is_pdf:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type:
                print(f"Warning: Expected PDF, but got Content-Type: {content_type} for {url}")
                # Decide if you want to proceed or return failure
                # For now, we'll try to save it anyway.

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {destination_path}")
        return True, destination_path
    except IOError as e:
        print(f"Error saving file to {destination_path}: {e}")
        return False, str(e)


def extract_text_from_pdf(pdf_path):
    """Extracts text from a PDF file."""
    print(f"Extracting text from PDF: {pdf_path}")
    if not os.path.exists(pdf_path):
        print(f"PDF file not found: {pdf_path}")
        return None, "PDF file not found"
    try:
        reader = pypdf.PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        if not text.strip():
            return None, "No text extracted from PDF (possibly image-based or empty)"
        print(
            f"Successfully extracted text from {pdf_path} (length: {len(text)})")
        return text, None
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None, str(e)


def extract_hyperlink_table_data(pdf_path, start_page=None, end_page=None):
    """
    Extracts groups of hyperlinks and their single associated table from a PDF,
    optionally for a specific page range.
    Also returns a list of hyperlinks that did not have a table immediately following them.
    A table is associated with all hyperlinks that appear directly before it
    on the same page and after any previously processed table or its associated hyperlinks.
    Includes 'approval_text' (e.g., "Aprovado", "Rejeitado") found after tables or unpaired links.
    
    One approval text can apply to multiple proposals that come before it in sequence.
    
    Deduplicates proposals where the same proposal number appears multiple times,
    keeping only versions with approval text when duplicates exist.

    Args:
        pdf_path (str): The path to the PDF file.
        start_page (int, optional): The 1-indexed start page of the range to process. 
                                    If None, processing starts from the first page.
        end_page (int, optional): The 1-indexed end page of the range to process.
                                  If None, processing goes up to the last page.

    Returns:
        tuple: A tuple containing two lists:
               - extracted_pairs (list): A list of dictionaries, where each dictionary
                 contains 'hyperlinks' (a list of hyperlink dicts), 'table_data' (a pandas DataFrame),
                 'page_num' (1-indexed), 'table_bottom_y' (float), and 'approval_text' (str|None).
               - unpaired_hyperlinks (list): A list of dictionaries for hyperlinks
                 that were not associated with any table, including 'hyperlink_text', 
                 'uri', 'page_num' (1-indexed), 'rect_y1' (float), and 'approval_text' (str|None).
    """
    extracted_pairs = []
    unpaired_hyperlinks_all = [] # Temp list to hold all unpaired links before final processing
    doc_fitz = fitz.open(pdf_path)

    num_doc_pages = len(doc_fitz)

    first_page_to_process_0idx = 0
    if start_page is not None:
        first_page_to_process_0idx = max(0, start_page - 1)

    last_page_to_process_0idx = num_doc_pages - 1
    if end_page is not None:
        last_page_to_process_0idx = min(num_doc_pages - 1, end_page - 1)

    if first_page_to_process_0idx >= num_doc_pages or first_page_to_process_0idx > last_page_to_process_0idx:
        doc_fitz.close()
        return [], []

    for page_num_0idx in range(first_page_to_process_0idx, last_page_to_process_0idx + 1):
        page_fitz = doc_fitz[page_num_0idx]
        current_page_1idx = page_num_0idx + 1

        # Extract potential approval lines from the current page
        approval_keywords = ["aprovad", "rejeitad", "prejudicad"]
        page_approval_lines = []
        page_text_dict = page_fitz.get_text("dict", sort=True)
        for block in page_text_dict.get("blocks", []):
            if block.get("type") == 0:  # text block
                for line in block.get("lines", []):
                    line_text_parts = [span["text"] for span in line.get("spans", [])]
                    full_line_text = "".join(line_text_parts).strip()
                    line_bbox = line["bbox"]  # (x0, y0, x1, y1)

                    if any(keyword in full_line_text.lower() for keyword in approval_keywords) and full_line_text:
                        page_approval_lines.append({
                            'text': full_line_text,
                            'y0': line_bbox[1], # top y-coordinate of the line
                            'y1': line_bbox[3], # bottom y-coordinate of the line
                            'used': False # Mark if this line gets associated
                        })
        page_approval_lines.sort(key=lambda apl: apl['y0'])

        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link['kind'] == fitz.LINK_URI:
                uri = link['uri']
                if ".pdf" in uri.lower(): # Skip links to other PDFs
                    continue
                rect = link['from']
                link_text_raw = page_fitz.get_text("text", clip=rect)
                link_text = ' '.join(link_text_raw.split()) if link_text_raw else "N/A"


                page_hyperlinks.append({
                    'text': link_text,
                    'uri': uri,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1), # (x0, y0, x1, y1)
                    'page_num_fitz': page_num_0idx
                })
        page_hyperlinks.sort(key=lambda h: h['rect'][1]) # Sort by y0

        page_tables_data = []
        try:
            tables_on_page_json = tabula.read_pdf(pdf_path,
                                                  pages=str(current_page_1idx),
                                                  output_format="json",
                                                  multiple_tables=True,
                                                  lattice=True,
                                                  silent=True)
            if not tables_on_page_json: # Try stream if lattice fails
                tables_on_page_json = tabula.read_pdf(pdf_path,
                                                      pages=str(current_page_1idx),
                                                      output_format="json",
                                                      multiple_tables=True,
                                                      stream=True,
                                                      silent=True)
        except Exception:
            tables_on_page_json = []
        
        for table_json_data in tables_on_page_json:
            table_rows_text = []
            if table_json_data['data']:
                for row_obj in table_json_data['data']:
                    current_row = [cell['text'] for cell in row_obj]
                    table_rows_text.append(current_row)
            if not table_rows_text:
                continue
            df = pd.DataFrame(table_rows_text)
            page_tables_data.append({
                'dataframe': df,
                'top': table_json_data['top'],
                'left': table_json_data['left'],
                'bottom': table_json_data['top'] + table_json_data['height'],
                'right': table_json_data['left'] + table_json_data['width'],
                'page_num_fitz': page_num_0idx
            })
        page_tables_data.sort(key=lambda t: t['top'])

        # Correlate hyperlinks and tables first
        hyperlink_cursor = 0
        temp_unpaired_links_on_page = []

        for table_idx in range(len(page_tables_data)):
            table = page_tables_data[table_idx]
            table_top_y = table['top']
            table_bottom_y = table['bottom']
            
            # Collect hyperlinks that are above the current table and after the previous table/hyperlinks
            links_for_current_table = []
            start_hyperlink_cursor_for_table = hyperlink_cursor
            while hyperlink_cursor < len(page_hyperlinks):
                hyperlink = page_hyperlinks[hyperlink_cursor]
                hyperlink_bottom_y = hyperlink['rect'][3] # y1 of hyperlink
                if hyperlink_bottom_y < table_top_y:
                    links_for_current_table.append({
                        'text': hyperlink['text'],
                        'uri': hyperlink['uri']
                    })
                    hyperlink_cursor += 1
                else:
                    break # Hyperlink is below or overlapping the table start

            if links_for_current_table:
                extracted_pairs.append({
                    'hyperlinks': links_for_current_table,
                    'table_data': table['dataframe'],
                    'page_num': current_page_1idx,
                    'table_bottom_y': table_bottom_y,
                    'approval_text': None  # Will be assigned later
                })
            else: # No hyperlinks above this table
                for i in range(start_hyperlink_cursor_for_table, hyperlink_cursor):
                    h = page_hyperlinks[i]
                    temp_unpaired_links_on_page.append({
                        'hyperlink_text': h['text'],
                        'uri': h['uri'],
                        'page_num': current_page_1idx,
                        'rect_y1': h['rect'][3], # bottom y of hyperlink rect
                        'approval_text': None # Will be assigned later
                    })

        # Add remaining hyperlinks (those after all tables or if no tables) to temp_unpaired_links_on_page
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            temp_unpaired_links_on_page.append({
                'hyperlink_text': hyperlink['text'],
                'uri': hyperlink['uri'],
                'page_num': current_page_1idx,
                'rect_y1': hyperlink['rect'][3],
                'approval_text': None # Will be assigned later
            })
        
        # Now assign approval text to both extracted pairs and unpaired links on this page
        # Create a combined list of all content items with their positions for approval assignment
        all_content_items = []
        
        # Add extracted pairs
        for pair in extracted_pairs:
            if pair['page_num'] == current_page_1idx:
                all_content_items.append({
                    'type': 'extracted_pair',
                    'item': pair,
                    'bottom_y': pair['table_bottom_y']
                })
        
        # Add unpaired links
        for unpaired_link in temp_unpaired_links_on_page:
            all_content_items.append({
                'type': 'unpaired_link',
                'item': unpaired_link,
                'bottom_y': unpaired_link['rect_y1']
            })
        
        # Sort all content by bottom position
        all_content_items.sort(key=lambda x: x['bottom_y'])
        
        # Assign approval text using a more sophisticated approach
        # Each approval text applies to all preceding content items since the last approval text
        content_cursor = 0
        
        for approval_line in page_approval_lines:
            approval_y = approval_line['y0']
            approval_text = approval_line['text']
            
            # Find all content items that come before this approval text
            items_for_this_approval = []
            
            # Start from content_cursor and find items that should get this approval
            while content_cursor < len(all_content_items):
                content_item = all_content_items[content_cursor]
                
                # If this content item is above the approval text, it gets the approval
                if content_item['bottom_y'] < approval_y:
                    items_for_this_approval.append(content_item)
                    content_cursor += 1
                else:
                    # This content item is below the approval, stop here
                    break
            
            # Assign approval text to all items found
            for content_item in items_for_this_approval:
                content_item['item']['approval_text'] = approval_text
            
            if items_for_this_approval:
                # print(f"Assigned approval '{approval_text}' to {len(items_for_this_approval)} items on page {current_page_1idx}")
                approval_line['used'] = True

        # Add unpaired links from this page to the global list
        unpaired_hyperlinks_all.extend(temp_unpaired_links_on_page)

    doc_fitz.close()
    
    # Deduplicate proposals across both lists - keep ones with approval text when duplicates exist
    extracted_pairs, unpaired_hyperlinks_all = _deduplicate_proposals_across_lists(extracted_pairs, unpaired_hyperlinks_all)
    
    return extracted_pairs, unpaired_hyperlinks_all


def _extract_proposal_number(text):
    """
    Extracts proposal number from text (e.g., "371/XVI" from various text formats).
    Returns None if no proposal number is found.
    """
    import re
    # Look for pattern like numbers/roman numerals (e.g., 371/XVI, 123/XV, etc.)
    pattern = r'(\d+/[IVX]+)'
    match = re.search(pattern, text)
    return match.group(1) if match else None


def _deduplicate_proposals_across_lists(extracted_pairs, unpaired_hyperlinks):
    """
    Deduplicates proposals across both extracted_pairs and unpaired_hyperlinks lists.
    When duplicates exist, keeps only items with approval_text.
    If all duplicates have approval_text or none have it, keeps all duplicates.
    
    Args:
        extracted_pairs: List of extracted pair dictionaries
        unpaired_hyperlinks: List of unpaired hyperlink dictionaries
    
    Returns:
        tuple: (deduplicated_extracted_pairs, deduplicated_unpaired_hyperlinks)
    """
    # First, collect all proposal numbers and their sources
    all_proposal_items = []
    
    # Add extracted pairs
    for item in extracted_pairs:
        proposal_numbers = set()
        for hyperlink in item['hyperlinks']:
            prop_num = _extract_proposal_number(hyperlink['text'])
            if prop_num:
                proposal_numbers.add(prop_num)
        
        for prop_num in proposal_numbers:
            all_proposal_items.append({
                'proposal_number': prop_num,
                'item': item,
                'source': 'extracted_pairs',
                'has_approval': bool(item.get('approval_text'))
            })
    
    # Add unpaired hyperlinks
    for item in unpaired_hyperlinks:
        prop_num = _extract_proposal_number(item['hyperlink_text'])
        if prop_num:
            all_proposal_items.append({
                'proposal_number': prop_num,
                'item': item,
                'source': 'unpaired_hyperlinks',
                'has_approval': bool(item.get('approval_text'))
            })
    
    # Group by proposal number
    proposal_groups = {}
    for prop_item in all_proposal_items:
        prop_num = prop_item['proposal_number']
        if prop_num not in proposal_groups:
            proposal_groups[prop_num] = []
        proposal_groups[prop_num].append(prop_item)
    
    # Determine which items to keep
    items_to_keep = set()  # Use set to store (id, source) tuples
    
    for prop_num, group_items in proposal_groups.items():
        if len(group_items) == 1:
            # No duplicates, keep as-is
            item = group_items[0]['item']
            items_to_keep.add((id(item), group_items[0]['source']))
        else:
            # Handle duplicates
            items_with_approval = [gi for gi in group_items if gi['has_approval']]
            items_without_approval = [gi for gi in group_items if not gi['has_approval']]
            
            if items_with_approval:
                # Keep only items with approval text
                for gi in items_with_approval:
                    items_to_keep.add((id(gi['item']), gi['source']))
                print(f"Deduplicated proposal {prop_num}: kept {len(items_with_approval)} items with approval text, "
                      f"removed {len(items_without_approval)} items without approval text")
            else:
                # All items lack approval text, keep all
                for gi in group_items:
                    items_to_keep.add((id(gi['item']), gi['source']))
                print(f"Proposal {prop_num} has {len(group_items)} duplicates but none have approval text - keeping all")
    
    # Filter the original lists
    deduplicated_extracted_pairs = [item for item in extracted_pairs if (id(item), 'extracted_pairs') in items_to_keep]
    deduplicated_unpaired_hyperlinks = [item for item in unpaired_hyperlinks if (id(item), 'unpaired_hyperlinks') in items_to_keep]
    
    # Add items without proposal numbers back
    for item in extracted_pairs:
        has_proposal = False
        for hyperlink in item['hyperlinks']:
            if _extract_proposal_number(hyperlink['text']):
                has_proposal = True
                break
        if not has_proposal and item not in deduplicated_extracted_pairs:
            deduplicated_extracted_pairs.append(item)
    
    for item in unpaired_hyperlinks:
        if not _extract_proposal_number(item['hyperlink_text']) and item not in deduplicated_unpaired_hyperlinks:
            deduplicated_unpaired_hyperlinks.append(item)
    
    return deduplicated_extracted_pairs, deduplicated_unpaired_hyperlinks

def generate_session_pdf_filename(session_pdf_url, session_year_param):
    """Generate a safe, descriptive filename for session PDFs."""
    try:
        parsed_url = urlparse(session_pdf_url)
        query_params = parse_qs(parsed_url.query)

        original_filename = None
        if 'Fich' in query_params and query_params['Fich'][0]:
            original_filename = query_params['Fich'][0]
        elif 'Nomeficheiro' in query_params and query_params['Nomeficheiro'][0]:
            original_filename = query_params['Nomeficheiro'][0]

        if original_filename:
            safe_filename_base = re.sub(r'[^\w\-_.]', '_', original_filename)
            safe_filename_base = re.sub(r'\.+', '.', safe_filename_base)
            safe_filename_base = re.sub(r'_+', '_', safe_filename_base)
            safe_filename_base = safe_filename_base.strip('._')

            if len(safe_filename_base) > 100:
                name_part, ext_part = os.path.splitext(safe_filename_base)
                ext_part = ext_part if ext_part else '.pdf'
                name_part = name_part[:100 - len(ext_part)]
                safe_filename = name_part + ext_part
            else:
                safe_filename = safe_filename_base
        else:
            url_hash = hashlib.md5(session_pdf_url.encode()).hexdigest()[:10]
            safe_filename = f"session_{session_year_param}_{url_hash}.pdf"

        if not safe_filename.lower().endswith(('.pdf', '.doc', '.docx')):
            safe_filename_base, _ = os.path.splitext(safe_filename)
            safe_filename = safe_filename_base + '.pdf'

        if not safe_filename.startswith(str(session_year_param)):
            final_filename = f"{session_year_param}_{safe_filename}"
        else:
            final_filename = safe_filename

        final_filename = re.sub(r'_+', '_', final_filename)
        return final_filename

    except Exception as e:
        print(
            f"Error generating session PDF filename for {session_pdf_url}: {e}. Using fallback.")
        url_hash = hashlib.md5(session_pdf_url.encode()).hexdigest()[:10]
        return f"session_{session_year_param}_{url_hash}_fallback.pdf"



import os
import glob

def validate_hyperlink_extraction():
    """
    Validates the extract_hyperlink_table_data function by testing it on all PDFs
    in the data/session_pdfs/ directory.
    """
    # Define the path to session PDFs
    pdf_directory = "data/session_pdfs"
    pdf_pattern = os.path.join(pdf_directory, "*.pdf")
    
    # Get all PDF files
    pdf_files = glob.glob(pdf_pattern)
    
    if not pdf_files:
        print(f"No PDF files found in {pdf_directory}")
        return
    
    print(f"Found {len(pdf_files)} PDF files to validate")
    print("=" * 80)
    
    for i, pdf_path in enumerate(pdf_files, 1):
        pdf_filename = os.path.basename(pdf_path)
        print(f"\n[{i}/{len(pdf_files)}] Processing: {pdf_filename}")
        print("-" * 60)
        
        try:
            # Extract hyperlinks and table data
            extracted_pairs, unpaired_hyperlinks = extract_hyperlink_table_data(pdf_path)
            
            # Display results
            print(f"✓ Extraction successful")
            print(f"  • Hyperlink-table pairs found: {len(extracted_pairs)}")
            print(f"  • Unpaired hyperlinks found: {len(unpaired_hyperlinks)}")
            
            # Show details of extracted pairs
            if extracted_pairs:
                print("\n  Hyperlink-Table Pairs:")
                for j, pair in enumerate(extracted_pairs):
                    print(f"    [{j+1}] Page {pair['page_num']}: {len(pair['hyperlinks'])} hyperlinks, "
                          f"table shape: {pair['table_data'].shape}")
                    print(f"        Approval text: {pair.get('approval_text', 'None')}")
                    for k, hyperlink in enumerate(pair['hyperlinks']):
                        truncated_text = hyperlink['text'][:50] + "..." if len(hyperlink['text']) > 50 else hyperlink['text']
                        print(f"        Link {k+1}: {truncated_text}")
            
            # Show details of unpaired hyperlinks
            if unpaired_hyperlinks:
                print("\n  Unpaired Hyperlinks:")
                for j, link in enumerate(unpaired_hyperlinks):
                    truncated_text = link['hyperlink_text'][:50] + "..." if len(link['hyperlink_text']) > 50 else link['hyperlink_text']
                    approval = link.get('approval_text', 'None')
                    print(f"    [{j+1}] Page {link['page_num']}: {truncated_text}")
                    print(f"        Approval text: {approval}")
            
            if not extracted_pairs and not unpaired_hyperlinks:
                print("  ⚠️  No hyperlinks or tables found in this PDF")
                
        except Exception as e:
            print(f"❌ Error processing {pdf_filename}: {str(e)}")
            print(f"   Error type: {type(e).__name__}")
        
        # Add a breakpoint here for debugging individual files
        print("\n" + "="*80)
        # Uncomment the line below if you want to pause after each file
        # input("Press Enter to continue to next PDF...")

if __name__ == "__main__":
    validate_hyperlink_extraction()