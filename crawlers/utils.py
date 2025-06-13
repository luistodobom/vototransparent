import os
import re
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
    DATAFRAME_PATH
)

def init_directories():
    """Creates necessary data directories if they don't exist."""
    os.makedirs(SESSION_PDF_DIR, exist_ok=True)
    os.makedirs(PROPOSAL_DOC_DIR, exist_ok=True)
    print(f"Ensured directories exist: {SESSION_PDF_DIR}, {PROPOSAL_DOC_DIR}")


def load_or_initialize_dataframe():
    """Loads the DataFrame from CSV if it exists, otherwise initializes an empty one."""
    if os.path.exists(DATAFRAME_PATH):
        print(f"Loading existing DataFrame from {DATAFRAME_PATH}")
        try:
            df = pd.read_csv(DATAFRAME_PATH)
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


def save_dataframe(df):
    """Saves the DataFrame to CSV."""
    try:
        df.to_csv(DATAFRAME_PATH, index=False)
        print(f"DataFrame saved to {DATAFRAME_PATH}")
    except Exception as e:
        print(f"Error saving DataFrame: {e}")


def download_file(url, destination_path, is_pdf=True):
    """Downloads a file from a URL to a destination path."""
    print(f"Attempting to download: {url} to {destination_path}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers,
                                timeout=DOWNLOAD_TIMEOUT, stream=True)
        response.raise_for_status()

        # Check content type for PDFs if expected
        if is_pdf:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type:
                print(
                    f"Warning: Expected PDF, but got Content-Type: {content_type} for {url}")
                # Decide if you want to proceed or return failure
                # For now, we'll try to save it anyway.

        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {destination_path}")
        return True, destination_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False, str(e)
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

        # Correlate hyperlinks and tables, then assign approval text
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
                # Find approval text for this table group
                approval_text_for_table = None
                min_diff_y = float('inf')
                assigned_approval_idx = -1
                for ap_idx, ap_line in enumerate(page_approval_lines):
                    if not ap_line['used'] and ap_line['y0'] > table_bottom_y:
                        diff = ap_line['y0'] - table_bottom_y
                        if diff < min_diff_y:
                            min_diff_y = diff
                            approval_text_for_table = ap_line['text']
                            assigned_approval_idx = ap_idx
                        # Optimization: if sorted, first one might be good enough if not too far
                        # For now, find closest
                
                if assigned_approval_idx != -1:
                    page_approval_lines[assigned_approval_idx]['used'] = True

                extracted_pairs.append({
                    'hyperlinks': links_for_current_table,
                    'table_data': table['dataframe'],
                    'page_num': current_page_1idx,
                    'table_bottom_y': table_bottom_y,
                    'approval_text': approval_text_for_table
                })
            else: # No hyperlinks above this table, process any preceding hyperlinks as unpaired
                for i in range(start_hyperlink_cursor_for_table, hyperlink_cursor):
                    # These were considered but not part of a group if links_for_current_table is empty
                    # This case needs careful handling, usually links_for_current_table won't be empty if hyperlink_cursor advanced
                    # For safety, add them to temp_unpaired_links_on_page
                    h = page_hyperlinks[i]
                    temp_unpaired_links_on_page.append({
                        'hyperlink_text': h['text'],
                        'uri': h['uri'],
                        'page_num': current_page_1idx,
                        'rect_y1': h['rect'][3], # bottom y of hyperlink rect
                        'approval_text': None # Will be assigned below
                    })


        # Add remaining hyperlinks (those after all tables or if no tables) to temp_unpaired_links_on_page
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            temp_unpaired_links_on_page.append({
                'hyperlink_text': hyperlink['text'],
                'uri': hyperlink['uri'],
                'page_num': current_page_1idx,
                'rect_y1': hyperlink['rect'][3],
                'approval_text': None # Will be assigned below
            })
        
        # Assign approval text to unpaired links on this page
        temp_unpaired_links_on_page.sort(key=lambda x: x['rect_y1']) # Sort by their bottom position

        for unpaired_link in temp_unpaired_links_on_page:
            hyperlink_bottom_y = unpaired_link['rect_y1']
            approval_text_for_link = None
            min_diff_y = float('inf')
            assigned_approval_idx = -1

            for ap_idx, ap_line in enumerate(page_approval_lines):
                if not ap_line['used'] and ap_line['y0'] > hyperlink_bottom_y:
                    diff = ap_line['y0'] - hyperlink_bottom_y
                    if diff < min_diff_y:
                        min_diff_y = diff
                        approval_text_for_link = ap_line['text']
                        assigned_approval_idx = ap_idx
            
            if assigned_approval_idx != -1:
                page_approval_lines[assigned_approval_idx]['used'] = True
            
            unpaired_link['approval_text'] = approval_text_for_link
            unpaired_hyperlinks_all.append(unpaired_link)

    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks_all

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
