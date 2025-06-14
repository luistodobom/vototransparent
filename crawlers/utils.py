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


def _deduplicate_hyperlinks(hyperlinks):
    """
    Deduplicates hyperlinks based on URI, keeping the best one according to criteria:
    1. Keep the one that contains unique proposal identifier (format: ^\\d+\\/[IVXLCDM]+$)
    2. If multiple contain identifier, keep the longest string
    
    Args:
        hyperlinks: List of hyperlink dictionaries with 'text' and 'uri' keys
    
    Returns:
        List of deduplicated hyperlinks
    """
    import re
    
    if not hyperlinks:
        return hyperlinks
    
    # Group hyperlinks by URI
    uri_groups = {}
    for hyperlink in hyperlinks:
        uri = hyperlink['uri']
        if uri not in uri_groups:
            uri_groups[uri] = []
        uri_groups[uri].append(hyperlink)
    
    # For each URI group, select the best hyperlink
    deduplicated = []
    proposal_id_pattern = r'\d+\/[IVXLCDM]+'  # Pattern to find proposal IDs within text
    
    for uri, group in uri_groups.items():
        if len(group) == 1:
            # No duplicates for this URI
            deduplicated.extend(group)
        else:
            # Multiple hyperlinks with same URI - apply selection criteria
            hyperlinks_with_proposal_id = []
            hyperlinks_without_proposal_id = []
            
            for hyperlink in group:
                text = hyperlink['text']
                # Check if text contains proposal identifier
                if re.search(proposal_id_pattern, text):
                    hyperlinks_with_proposal_id.append(hyperlink)
                else:
                    hyperlinks_without_proposal_id.append(hyperlink)
            
            if hyperlinks_with_proposal_id:
                # Keep the longest one among those with proposal ID
                best_hyperlink = max(hyperlinks_with_proposal_id, key=lambda h: len(h['text']))
                deduplicated.append(best_hyperlink)
                print(f"Deduplicated URI {uri}: kept hyperlink with proposal ID (length {len(best_hyperlink['text'])}): '{best_hyperlink['text'][:50]}...'")
            else:
                # No hyperlinks with proposal ID, keep the longest one
                best_hyperlink = max(hyperlinks_without_proposal_id, key=lambda h: len(h['text']))
                deduplicated.append(best_hyperlink)
                print(f"Deduplicated URI {uri}: kept longest hyperlink (length {len(best_hyperlink['text'])}): '{best_hyperlink['text'][:50]}...'")
    
    return deduplicated


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
    using approval text as primary delimiters to create blocks of proposals/tables.
    
    Strategy:
    1. Use approval text (Aprovado/Rejeitado/Prejudicado) as primary delimiters between blocks
    2. Each block can contain hyperlinks and optionally a table
    3. If multiple tables exist without approval text between them, tables serve as secondary delimiters
    4. Hyperlinks in a block are associated with the table in that same block (if any)
    
    Args:
        pdf_path (str): The path to the PDF file.
        start_page (int, optional): The 1-indexed start page of the range to process. 
        end_page (int, optional): The 1-indexed end page of the range to process.

    Returns:
        tuple: A tuple containing two lists:
               - extracted_pairs (list): A list of dictionaries with 'hyperlinks', 'table_data', 
                 'page_num', 'table_bottom_y', and 'approval_text'.
               - unpaired_hyperlinks (list): A list of dictionaries for hyperlinks without tables,
                 including 'hyperlink_text', 'uri', 'page_num', 'rect_y1', and 'approval_text'.
    """
    extracted_pairs = []
    unpaired_hyperlinks_all = []
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

    # Collect all elements across all pages first
    all_elements = []
    
    for page_num_0idx in range(first_page_to_process_0idx, last_page_to_process_0idx + 1):
        page_fitz = doc_fitz[page_num_0idx]
        current_page_1idx = page_num_0idx + 1

        # Extract approval lines - these are our primary delimiters
        approval_keywords = ["aprovad", "rejeitad", "prejudicad"]
        page_text_dict = page_fitz.get_text("dict", sort=True)
        for block in page_text_dict.get("blocks", []):
            if block.get("type") == 0:  # text block
                for line in block.get("lines", []):
                    line_text_parts = [span["text"] for span in line.get("spans", [])]
                    full_line_text = "".join(line_text_parts).strip()
                    line_bbox = line["bbox"]  # (x0, y0, x1, y1)

                    if any(keyword in full_line_text.lower() for keyword in approval_keywords) and full_line_text:
                        # More restrictive approval text detection
                        # Should be relatively short and primarily approval text
                        if len(full_line_text) <= 50 and any(
                            full_line_text.lower().strip().startswith(keyword) for keyword in approval_keywords
                        ):
                            all_elements.append({
                                'type': 'approval',
                                'text': full_line_text,
                                'page_num': current_page_1idx,
                                'y_position': line_bbox[1],  # Use top y-coordinate
                                'y_bottom': line_bbox[3]
                            })

        # Extract hyperlinks
        links = page_fitz.get_links()
        for link in links:
            if link['kind'] == fitz.LINK_URI:
                uri = link['uri']
                if ".pdf" in uri.lower():  # Skip links to other PDFs
                    continue
                rect = link['from']
                link_text_raw = page_fitz.get_text("text", clip=rect)
                link_text = ' '.join(link_text_raw.split()) if link_text_raw else "N/A"

                all_elements.append({
                    'type': 'hyperlink',
                    'text': link_text,
                    'uri': uri,
                    'page_num': current_page_1idx,
                    'y_position': rect.y0,
                    'y_bottom': rect.y1,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1)
                })

        # Extract tables
        try:
            tables_on_page_json = tabula.read_pdf(pdf_path,
                                                  pages=str(current_page_1idx),
                                                  output_format="json",
                                                  multiple_tables=True,
                                                  lattice=True,
                                                  silent=True)
            if not tables_on_page_json:
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
            
            # Check if this is a title table containing "VOTAÇÕES EFETUADAS EM"
            table_text = ' '.join([' '.join(row) for row in table_rows_text]).upper()
            if "VOTAÇÕES EFETUADAS EM" in table_text:
                print(f"Skipping title table on page {current_page_1idx}: contains 'VOTAÇÕES EFETUADAS EM'")
                continue
            
            all_elements.append({
                'type': 'table',
                'dataframe': df,
                'page_num': current_page_1idx,
                'y_position': table_json_data['top'],
                'y_bottom': table_json_data['top'] + table_json_data['height'],
                'table_data': {
                    'top': table_json_data['top'],
                    'left': table_json_data['left'],
                    'bottom': table_json_data['top'] + table_json_data['height'],
                    'right': table_json_data['left'] + table_json_data['width']
                }
            })

    doc_fitz.close()
    
    # Sort all elements by page and y-position
    all_elements.sort(key=lambda x: (x['page_num'], x['y_position']))
    
    # Create blocks using approval text as primary delimiters and tables as secondary delimiters
    blocks = []
    current_block = {'hyperlinks': [], 'tables': [], 'approval_text': None}
    
    for element in all_elements:
        if element['type'] == 'approval':
            # Approval text always ends the current block and creates a new one
            if current_block['hyperlinks'] or current_block['tables']:
                current_block['approval_text'] = element['text']
                blocks.append(current_block)
            else:
                # If current block is empty, this approval belongs to the previous block
                if blocks:
                    blocks[-1]['approval_text'] = element['text']
            
            # Start new block after approval text
            current_block = {'hyperlinks': [], 'tables': [], 'approval_text': None}
        
        elif element['type'] == 'hyperlink':
            current_block['hyperlinks'].append(element)
        
        elif element['type'] == 'table':
            # If we already have a table in current block, start a new block (secondary delimiter)
            if current_block['tables']:
                # End current block and start new one
                blocks.append(current_block)
                current_block = {'hyperlinks': [], 'tables': [], 'approval_text': None}
            
            current_block['tables'].append(element)
    
    # Add final block if it has content
    if current_block['hyperlinks'] or current_block['tables']:
        blocks.append(current_block)
    
    # Process blocks to create final output
    for block in blocks:
        # Deduplicate hyperlinks within this block
        deduplicated_hyperlinks = _deduplicate_hyperlinks(block['hyperlinks'])
        hyperlinks_for_output = [{'text': h['text'], 'uri': h['uri']} for h in deduplicated_hyperlinks]
        
        if block['tables']:
            # Block has table(s) - create extracted pairs
            for table_element in block['tables']:
                extracted_pairs.append({
                    'hyperlinks': hyperlinks_for_output,
                    'table_data': table_element['dataframe'],
                    'page_num': table_element['page_num'],
                    'table_bottom_y': table_element['y_bottom'],
                    'approval_text': block['approval_text']
                })
        elif hyperlinks_for_output:
            # Block has hyperlinks but no table - add to unpaired
            for hyperlink in deduplicated_hyperlinks:
                unpaired_hyperlinks_all.append({
                    'hyperlink_text': hyperlink['text'],
                    'uri': hyperlink['uri'],
                    'page_num': hyperlink['page_num'],
                    'rect_y1': hyperlink['y_bottom'],
                    'approval_text': block['approval_text']
                })
    
    # Deduplicate proposals across both lists
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
    deduplicated_extracted_pairs_ids = {id(item) for item in deduplicated_extracted_pairs}
    deduplicated_unpaired_hyperlinks_ids = {id(item) for item in deduplicated_unpaired_hyperlinks}
    
    for item in extracted_pairs:
        has_proposal = False
        for hyperlink in item['hyperlinks']:
            if _extract_proposal_number(hyperlink['text']):
                has_proposal = True
                break
        if not has_proposal and id(item) not in deduplicated_extracted_pairs_ids:
            deduplicated_extracted_pairs.append(item)
    
    for item in unpaired_hyperlinks:
        if not _extract_proposal_number(item['hyperlink_text']) and id(item) not in deduplicated_unpaired_hyperlinks_ids:
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
    
    pdf_files = ["/Users/luistb/Downloads/XVI_1_67_2024-12-12_ResultadoVotacoes_2024-12-12.pdf"]
    pdf_files = ["data/session_pdfs/2023_XV_2_2_2023-09-19_ResultadoVotacoes_2023-09-19_Moção_Censura_.pdf"]
    pdf_files = ["data/session_pdfs/XV_1_70_2022-12-22_ResultadoVotacoes_2022-12-22.pdf"]
    pdf_files = ["data/session_pdfs/2023_XV_2_31_2023-12-19_ResultadoVotacoes_2023-12-19_OD_.pdf"]
        
    
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
                    
                    # Print table contents
                    print(f"        Table contents:")
                    table_df = pair['table_data']
                    if not table_df.empty:
                        print(f"          Table shape: {table_df.shape}")
                        print(f"          Column headers: {list(table_df.columns)}")
                        # Print all rows of the table with better formatting
                        for idx, row in table_df.iterrows():
                            row_values = []
                            for cell in row:
                                cell_str = str(cell) if pd.notna(cell) else "NaN"
                                if len(cell_str) > 40:
                                    cell_str = cell_str[:37] + "..."
                                row_values.append(cell_str)
                            row_str = " | ".join(row_values)
                            print(f"          Row {idx}: {row_str}")
                    else:
                        print(f"          (Empty table)")
                    print()
            
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