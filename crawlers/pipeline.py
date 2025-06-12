import argparse
import os
import re
import time
import json
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pypdf
import pandas as pd
from dotenv import load_dotenv
import hashlib

# Add manual PDF parsing imports
import tabula
import fitz  # PyMuPDF

# Add Google GenAI SDK import
from google import genai
from google.genai import types

# --- Configuration ---
YEAR = 2021
DATAFRAME_PATH = f"data/parliament_data_{YEAR}.csv"
SESSION_PDF_DIR = "data/session_pdfs"
PROPOSAL_DOC_DIR = "data/proposal_docs"
DOWNLOAD_TIMEOUT = 60  # seconds for requests timeout
LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_DELAY = 5  # seconds
PDF_PAGE_PARTITION_SIZE = 5  # Process PDFs in chunks of this many pages

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY not found in .env file. Please create a .env file with your API key.")

# Initialize Gemini client
genai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# --- Utility Functions ---

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
            print(f"Warning: {DATAFRAME_PATH} is empty. Initializing a new DataFrame.")
            df = pd.DataFrame(columns=get_dataframe_columns())
        except Exception as e:
            print(f"Error loading DataFrame: {e}. Initializing a new DataFrame.")
            df = pd.DataFrame(columns=get_dataframe_columns())
    else:
        print("Initializing new DataFrame.")
        df = pd.DataFrame(columns=get_dataframe_columns())
    
    # Ensure all columns are present, add if missing (for schema evolution)
    expected_columns = get_dataframe_columns()
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA # Use pd.NA for missing values
    df = df[expected_columns] # Reorder columns to expected order
    
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
    """Downloads a file from a URL to a destination path.
    Returns 'Success' or an error message string."""
    print(f"Attempting to download: {url} to {destination_path}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT, stream=True)
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx status

        if is_pdf:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type:
                print(f"Warning: Expected PDF, but got Content-Type: {content_type} for {url}")
                # Continue to download, but this warning is logged.

        file_size = 0
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    file_size += len(chunk)
        
        if file_size == 0:
            print(f"Error: Downloaded file {destination_path} is empty for URL {url}.")
            # Optionally, remove the empty file:
            # try:
            #     os.remove(destination_path)
            # except OSError:
            #     pass
            return "Failed: Downloaded file is empty"

        print(f"Successfully downloaded {destination_path} ({file_size} bytes)")
        return "Success"

    except requests.exceptions.HTTPError as e:
        error_msg = f"Failed: HTTPError {e.response.status_code} for URL {url}"
        print(error_msg)
        return error_msg
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Failed: ConnectionError for URL {url} - {e}"
        print(error_msg)
        return error_msg
    except requests.exceptions.Timeout as e:
        error_msg = f"Failed: Timeout for URL {url} - {e}"
        print(error_msg)
        return error_msg
    except requests.exceptions.RequestException as e: # Catch-all for other request errors
        error_msg = f"Failed: RequestException {type(e).__name__} for URL {url} - {e}"
        print(error_msg)
        return error_msg
    except IOError as e:
        error_msg = f"Failed: IOError while saving to {destination_path} - {e}"
        print(error_msg)
        return error_msg
    except Exception as e: # Catch any other unexpected errors
        error_msg = f"Failed: Unexpected error downloading {url} - {type(e).__name__}: {e}"
        print(error_msg)
        return error_msg

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
        print(f"Successfully extracted text from {pdf_path} (length: {len(text)})")
        return text, None
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None, str(e)

def call_gemini_api(prompt_text, document_path=None, expect_json=False):
    """Calls the Gemini API with the given prompt and optional document file."""
    if not genai_client:
        return None, "GEMINI_API_KEY not configured"

    print(f"Calling Gemini API. Prompt length: {len(prompt_text)}")

    # Prepare contents array
    contents = [prompt_text]
    
    # If a document is provided, upload it using the File API
    if document_path and os.path.exists(document_path):
        try:
            print(f"Uploading file: {document_path}")
            uploaded_file = genai_client.files.upload(file=document_path)
            contents.append(uploaded_file)
            print(f"File uploaded successfully: {uploaded_file.name}")
        except Exception as e:
            return None, f"File upload failed: {e}"

    # Prepare generation config
    config = {}
    if expect_json:
        config = {
            "response_mime_type": "application/json",
        }

    for attempt in range(LLM_RETRY_ATTEMPTS):
        try:
            response = genai_client.models.generate_content(
                model="gemini-2.5-flash-preview-05-20",
                contents=contents,
                config=config if config else None
            )
            
            generated_text = response.text
            
            if not generated_text.strip():
                print(f"Gemini API Warning: Empty text response.")
                return None, "Empty text response from API"

            if expect_json:
                # Clean up JSON response if needed
                cleaned_text = generated_text.strip()
                if cleaned_text.startswith("```json"):
                    cleaned_text = cleaned_text[7:]
                if cleaned_text.endswith("```"):
                    cleaned_text = cleaned_text[:-3]
                
                try:
                    parsed_json = json.loads(cleaned_text)
                    print("Successfully parsed JSON response from Gemini API.")
                    return parsed_json, None
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from Gemini API response: {e}. Response text: {generated_text}")
                    return None, f"JSONDecodeError: {e}. Raw text: {generated_text[:500]}"
            
            print("Successfully received text response from Gemini API.")
            return generated_text, None

        except Exception as e:
            print(f"Error communicating with Gemini API (attempt {attempt + 1}/{LLM_RETRY_ATTEMPTS}): {e}")
            if attempt + 1 == LLM_RETRY_ATTEMPTS:
                return None, f"API error after {LLM_RETRY_ATTEMPTS} attempts: {e}"
        
        time.sleep(LLM_RETRY_DELAY)
    return None, f"Failed after {LLM_RETRY_ATTEMPTS} attempts."


# --- Script 1: Get PDFs (Session Summaries) ---
class ParliamentPDFScraper:
    def __init__(self):
        self.base_url = "https://www.parlamento.pt/ArquivoDocumentacao/Paginas/Arquivodevotacoes.aspx"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def get_page_content(self, year=None):
        try:
            url = f"{self.base_url}?ano={year}" if year else self.base_url
            print(f"Fetching session list for year: {year if year else 'current'}")
            response = self.session.get(url, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page for year {year}: {e}")
            return None
    
    def extract_pdf_links_from_html(self, html_content, year):
        if not html_content: return []
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Find all calendar detail containers that contain both date and PDF links
        calendar_details = soup.find_all('div', class_='row home_calendar hc-detail')
        
        for calendar_detail in calendar_details:
            # Extract date information
            date_elem = calendar_detail.find('p', class_='date')
            time_elem = calendar_detail.find('p', class_='time')
            
            session_date = None
            year_text_from_time_elem = None # Store year from time element
            if date_elem and time_elem:
                try:
                    day_month = date_elem.get_text(strip=True)  # e.g., "19.12"
                    year_text_from_time_elem = time_elem.get_text(strip=True)  # e.g., "2024"
                    
                    if '.' in day_month and year_text_from_time_elem.isdigit():
                        day, month = day_month.split('.')
                        # Convert to ISO date format (YYYY-MM-DD)
                        session_date = f"{year_text_from_time_elem}-{month.zfill(2)}-{day.zfill(2)}"
                except (ValueError, AttributeError) as e:
                    print(f"Error parsing date from {day_month} and {year_text_from_time_elem}: {e}")
                    session_date = None # Ensure session_date is None on error
            
            # Find PDF links within this calendar detail
            all_anchor_tags = calendar_detail.find_all('a', href=True)
            
            for link_tag in all_anchor_tags:
                href = link_tag.get('href', '')
                text_content = link_tag.get_text(strip=True)

                # Skip supplementary guides
                if "guião suplementar" in text_content.lower():
                    print(f"Skipping supplementary guide: {text_content} ({href})")
                    continue
                
                # Determine year for this link: use parsed year_text_from_time_elem if available, else fallback to function's year param
                link_year = int(year_text_from_time_elem) if year_text_from_time_elem and year_text_from_time_elem.isdigit() else year

                # Prioritize links that look like direct PDF links related to voting summaries
                if (href.lower().endswith('.pdf') and 
                    any(kw in href.lower() for kw in ['votacoe', 'resultado', 'dar', 'serieii'])): 
                    full_url = urljoin("https://www.parlamento.pt", href)
                    if "votaç" in text_content.lower() or "diário" in text_content.lower() or "reunião plenária" in text_content.lower():
                        pdf_links.append({
                            'url': full_url, 
                            'year': link_year,
                            'date': session_date, # Use parsed date if available for this calendar_detail
                            'text': text_content, 
                            'type': 'direct_pdf_votacao'
                        })
                # Parameterized links that often lead to PDFs
                elif ('doc.pdf' in href.lower() or 'path=' in href.lower() or 'downloadfile' in href.lower()):
                     if "votaç" in text_content.lower() or "diário" in text_content.lower():
                        full_url = urljoin("https://www.parlamento.pt", href)
                        pdf_links.append({
                            'url': full_url, 
                            'year': link_year,
                            'date': session_date, # Use parsed date if available for this calendar_detail
                            'text': text_content, 
                            'type': 'parameterized_pdf_votacao'
                        })
        
        # Deduplicate based on URL
        unique_links = []
        seen_urls = set()
        for link_info in pdf_links:
            if link_info['url'] not in seen_urls:
                unique_links.append(link_info)
                seen_urls.add(link_info['url'])
        
        print(f"Found {len(unique_links)} potential session PDF links for year {year}")
        return unique_links
    
    def scrape_years(self, start_year, end_year):
        print(f"Scraping session PDF links from {start_year} to {end_year}")
        all_pdf_links = []
        for year_to_scrape in range(start_year, end_year + 1): 
            html_content = self.get_page_content(year_to_scrape)
            if html_content:
                year_links = self.extract_pdf_links_from_html(html_content, year_to_scrape)
                all_pdf_links.extend(year_links)
            time.sleep(1) 
        return all_pdf_links

# --- Script 2: Get Votes (Parse Session PDF with LLM) ---

def extract_hyperlink_table_pairs_and_unpaired_links(pdf_path, session_year):
    """
    Extracts groups of hyperlinks and their single associated table from a PDF.
    If session_year < 2020, only extracts hyperlinks as unpaired_links.
    A table is associated with all hyperlinks that appear directly before it
    on the same page and after any previously processed table or its associated hyperlinks.
    """
    extracted_pairs = []
    unpaired_hyperlinks = []
    doc_fitz = fitz.open(pdf_path)

    for page_num in range(len(doc_fitz)):
        page_fitz = doc_fitz[page_num]
        
        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link.get('kind') == fitz.LINK_URI: # Check if it's a URI link
                link_rect = fitz.Rect(link['from']) # Convert tuple to Rect for y-coordinate
                page_hyperlinks.append({'text': link.get('uri', ''), 'uri': link.get('uri', ''), 'rect': link_rect, 'page_num': page_num})
        
        page_hyperlinks.sort(key=lambda h: h['rect'][1]) # Sort by vertical position

        if session_year >= 2020:
            page_tables_data = []
            try:
                # Extract tables using tabula-py for the current page
                # Adding explicit page number for tabula
                tables_on_page_json = tabula.read_pdf(pdf_path, pages=str(page_num + 1), multiple_tables=True, output_format="json", lattice=True, stream=True)
            except Exception as e:
                # print(f"Tabula failed to extract tables from page {page_num + 1}: {e}")
                tables_on_page_json = [] # Continue if tabula fails for a page

            for table_json_data in tables_on_page_json:
                # table_data = [[cell['text'] for cell in row] for row in table_json_data['data']]
                # For simplicity, storing the raw tabula output for now, can be processed later
                # Storing 'top' from the first cell of the first row if available
                table_top_coordinate = table_json_data['data'][0][0]['top'] if table_json_data['data'] and table_json_data['data'][0] else 0
                page_tables_data.append({'data': table_json_data, 'top': table_top_coordinate, 'page_num': page_num})
            
            page_tables_data.sort(key=lambda t: t['top'])

            hyperlink_cursor = 0 
            
            for table_idx in range(len(page_tables_data)):
                current_table = page_tables_data[table_idx]
                associated_hyperlinks_for_this_table = []
                
                # Find hyperlinks before this table and after the previous table's hyperlinks
                # Hyperlinks must be on the same page as the table
                while hyperlink_cursor < len(page_hyperlinks) and \
                      page_hyperlinks[hyperlink_cursor]['rect'][1] < current_table['top'] and \
                      page_hyperlinks[hyperlink_cursor]['page_num'] == current_table['page_num']:
                    associated_hyperlinks_for_this_table.append(page_hyperlinks[hyperlink_cursor])
                    hyperlink_cursor += 1
                
                if associated_hyperlinks_for_this_table:
                    extracted_pairs.append({
                        'hyperlinks': associated_hyperlinks_for_this_table,
                        'table': current_table['data'], # Store the raw table data
                        'page_num': current_table['page_num']
                    })
            
            # Add remaining hyperlinks on this page as unpaired
            for i in range(hyperlink_cursor, len(page_hyperlinks)):
                if page_hyperlinks[i]['page_num'] == page_num: # Ensure from current page
                    unpaired_hyperlinks.append(page_hyperlinks[i])
        else: # session_year < 2020
            unpaired_hyperlinks.extend(page_hyperlinks)


    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks


def extract_votes_from_session_pdf_text(session_pdf_path, session_year, session_date_str):
    """Enhanced voting extraction using manual PDF parsing followed by LLM processing."""
    print(f"Starting enhanced PDF parsing for: {session_pdf_path} (Year: {session_year})")
    
    try:
        doc_fitz = fitz.open(session_pdf_path)
        page_count = len(doc_fitz)
        doc_fitz.close()
        print(f"PDF has {page_count} pages")
        
        if page_count > PDF_PAGE_PARTITION_SIZE:
            print(f"PDF is long, processing in chunks. Year: {session_year}, Date: {session_date_str}")
            # Pass session_year and session_date_str to chunk processing
            return process_long_pdf_in_chunks(session_pdf_path, page_count, session_year, session_date_str)
    except Exception as e:
        print(f"Error checking PDF page count: {e}")
        # Fallback to processing as a whole if page count check fails, though it might be slow/fail
    
    try:
        # Pass session_year to the extraction function
        hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_pairs_and_unpaired_links(session_pdf_path, session_year)
        if session_year < 2020:
            print(f"Manual parsing (pre-2020 mode) found {len(unpaired_links)} unpaired links (hyperlinks only)")
        else:
            print(f"Manual parsing (post-2020 mode) found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")

    except Exception as e:
        print(f"Manual PDF parsing failed: {e}. Falling back to original text extraction method.")
        # Consider if fallback is appropriate or should just error out
        return None, f"Manual PDF parsing failed: {e}" 
    
    # Pass session_year to format_structured_data_for_llm
    structured_data_text = format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links, session_year)
    
    if not structured_data_text.strip() or "NO DATA" in structured_data_text: 
        print("No structured data extracted from PDF to send to LLM.")
        return [], "No structured data extracted" # Return empty list and message

    # Pass session_year and session_date_str to create_structured_data_prompt
    extracted_data, error = call_gemini_api(create_structured_data_prompt(structured_data_text, session_year, session_date_str), expect_json=True)
    if error:
        return None, error # Return None and error message
    if not isinstance(extracted_data, list): 
        if isinstance(extracted_data, dict) and 'proposal_name' in extracted_data:
            # Handle cases where LLM might return a single dict instead of a list of one
            extracted_data = [extracted_data]
        else:
            # print(f"LLM response was not a list as expected. Type: {type(extracted_data)}. Content: {extracted_data}")
            return None, f"LLM response format error: Expected a list, got {type(extracted_data)}"
    
    valid_proposals = validate_llm_proposals_response(extracted_data)
    
    if not valid_proposals and extracted_data: # Some data was returned but none validated
         # print(f"LLM returned data, but no proposals were validated. Raw: {extracted_data}")
         return [], "LLM data returned but no proposals validated" # Return empty list and message
    elif not valid_proposals and not extracted_data: # No data returned and none validated
        return [], "No proposals extracted by LLM" # Return empty list and message

    print(f"Successfully extracted {len(valid_proposals)} proposals using enhanced parsing method for year {session_year}")
    return valid_proposals, None

def process_long_pdf_in_chunks(session_pdf_path, page_count, session_year, session_date_str):
    """Process a long PDF by partitioning it into smaller chunks."""
    print(f"Processing long PDF ({page_count} pages) in chunks of {PDF_PAGE_PARTITION_SIZE} pages. Year: {session_year}")
    
    all_proposals = []
    partition_errors = []
    
    partitions = []
    start_page = 1  # tabula and fitz are 1-indexed for user-facing page numbers usually
    while start_page <= page_count:
        end_page = min(start_page + PDF_PAGE_PARTITION_SIZE - 1, page_count)
        partitions.append((start_page, end_page))
        start_page = end_page + 1
    
    print(f"Created {len(partitions)} partitions: {partitions}")
    
    for i, (start_page, end_page) in enumerate(partitions):
        print(f"Processing partition {i+1}/{len(partitions)}: pages {start_page}-{end_page}. Year: {session_year}")
        
        try:
            # Pass session_year to extract_hyperlink_table_pairs_for_page_range
            hyperlink_table_pairs_chunk, unpaired_links_chunk = extract_hyperlink_table_pairs_for_page_range(session_pdf_path, start_page, end_page, session_year)
            
            if not hyperlink_table_pairs_chunk and not unpaired_links_chunk:
                print(f"No data extracted from partition {start_page}-{end_page}.")
                continue

            # Pass session_year to format_structured_data_for_llm
            structured_data_text_chunk = format_structured_data_for_llm(hyperlink_table_pairs_chunk, unpaired_links_chunk, session_year)
            
            if not structured_data_text_chunk.strip() or "NO DATA" in structured_data_text_chunk:
                print(f"No structured data formatted for LLM from partition {start_page}-{end_page}.")
                continue

            # Pass session_year and session_date_str to create_structured_data_prompt
            extracted_data_chunk, error_chunk = call_gemini_api(create_structured_data_prompt(structured_data_text_chunk, session_year, session_date_str), expect_json=True)
            
            if error_chunk:
                print(f"Error in partition {start_page}-{end_page}: {error_chunk}")
                partition_errors.append(f"Partition {start_page}-{end_page}: {error_chunk}")
                continue
            
            if extracted_data_chunk and isinstance(extracted_data_chunk, list):
                valid_proposals_chunk = validate_llm_proposals_response(extracted_data_chunk)
                all_proposals.extend(valid_proposals_chunk)
                print(f"Added {len(valid_proposals_chunk)} proposals from partition {start_page}-{end_page}")
            elif extracted_data_chunk: # Not a list, but not an error string
                print(f"Unexpected data type from LLM for partition {start_page}-{end_page}: {type(extracted_data_chunk)}")
                partition_errors.append(f"Partition {start_page}-{end_page}: Unexpected LLM response type {type(extracted_data_chunk)}")

        except Exception as e:
            print(f"Unhandled error processing partition {start_page}-{end_page}: {e}")
            partition_errors.append(f"Partition {start_page}-{end_page}: Unhandled error {e}")
    
    if all_proposals:
        deduplicated_proposals = []
        seen_proposal_identifiers = set() # Using proposal_link as a unique identifier
        for proposal in all_proposals:
            identifier = proposal.get('proposal_link') # Or a combination of name and link
            if identifier and identifier not in seen_proposal_identifiers:
                deduplicated_proposals.append(proposal)
                seen_proposal_identifiers.add(identifier)
            elif not identifier: # If no link, consider adding based on name or just add all
                deduplicated_proposals.append(proposal) 
        
        print(f"Successfully extracted {len(deduplicated_proposals)} unique proposals from all partitions for year {session_year}")
        if partition_errors: 
             # print(f"Completed with errors in some partitions: {'; '.join(partition_errors)}")
             return deduplicated_proposals, f"Completed with errors: {'; '.join(partition_errors)}"
        return deduplicated_proposals, None
    elif partition_errors:
        # print(f"No proposals extracted, and errors occurred: {'; '.join(partition_errors)}")
        return None, f"No proposals extracted, errors: {'; '.join(partition_errors)}"
    else: 
        # print("No proposals found in any partition.")
        return [], "No proposals found in any partition"

def extract_hyperlink_table_pairs_for_page_range(pdf_path, start_page, end_page, session_year):
    """
    Extracts groups of hyperlinks and their single associated table for a specific page range in the PDF.
    If session_year < 2020, only extracts hyperlinks as unpaired_links.
    Note: start_page and end_page are 1-indexed.
    """
    extracted_pairs = []
    unpaired_hyperlinks = []
    doc_fitz = fitz.open(pdf_path)
    
    start_page_0idx = start_page - 1
    end_page_0idx = end_page - 1 # Inclusive
    
    for page_num_0idx in range(start_page_0idx, end_page_0idx + 1):
        if page_num_0idx >= len(doc_fitz): 
            # print(f"Warning: Page number {page_num_0idx + 1} is out of bounds for PDF with {len(doc_fitz)} pages.")
            break # Stop if page number exceeds document limits
            
        page_fitz = doc_fitz[page_num_0idx]
        current_page_1idx = page_num_0idx + 1 
        
        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link.get('kind') == fitz.LINK_URI:
                link_rect = fitz.Rect(link['from'])
                page_hyperlinks.append({'text': link.get('uri', ''), 'uri': link.get('uri', ''), 'rect': link_rect, 'page_num': current_page_1idx})
        
        page_hyperlinks.sort(key=lambda h: h['rect'][1]) 

        if session_year >= 2020:
            page_tables_data = []
            try:
                tables_on_page_json = tabula.read_pdf(pdf_path, pages=str(current_page_1idx), multiple_tables=True, output_format="json", lattice=True, stream=True)
            except Exception as e:
                # print(f"Tabula failed for page range on page {current_page_1idx}: {e}")
                tables_on_page_json = []

            for table_json_data in tables_on_page_json:
                table_top_coordinate = table_json_data['data'][0][0]['top'] if table_json_data['data'] and table_json_data['data'][0] else 0
                page_tables_data.append({'data': table_json_data, 'top': table_top_coordinate, 'page_num': current_page_1idx})
            
            page_tables_data.sort(key=lambda t: t['top'])

            hyperlink_cursor = 0 
            
            for table_idx in range(len(page_tables_data)):
                current_table = page_tables_data[table_idx]
                associated_hyperlinks_for_this_table = []
                
                while hyperlink_cursor < len(page_hyperlinks) and \
                      page_hyperlinks[hyperlink_cursor]['rect'][1] < current_table['top'] and \
                      page_hyperlinks[hyperlink_cursor]['page_num'] == current_table['page_num']: # Ensure same page
                    associated_hyperlinks_for_this_table.append(page_hyperlinks[hyperlink_cursor])
                    hyperlink_cursor += 1
                
                if associated_hyperlinks_for_this_table:
                    extracted_pairs.append({
                        'hyperlinks': associated_hyperlinks_for_this_table,
                        'table': current_table['data'],
                        'page_num': current_table['page_num']
                    })
            
            for i in range(hyperlink_cursor, len(page_hyperlinks)):
                if page_hyperlinks[i]['page_num'] == current_page_1idx: # Ensure from current page in range
                    unpaired_hyperlinks.append(page_hyperlinks[i])
        else: # session_year < 2020
            unpaired_hyperlinks.extend(page_hyperlinks)


    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks

def format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links, session_year):
    """Format the structured data for the LLM, accommodating grouped hyperlinks and pre-2020 format."""
    structured_data_text = ""
    has_data = False

    if session_year < 2020:
        structured_data_text += "PROPOSTAS EXTRAÍDAS DO PDF (SEM TABELAS DE VOTAÇÃO - ANTERIOR A 2020):\n"
        if unpaired_links:
            has_data = True
            for idx, link_info in enumerate(unpaired_links):
                text = link_info.get('text', 'N/A')
                uri = link_info.get('uri', 'N/A')
                page = link_info.get('page_num', 'N/A')
                structured_data_text += f"  PROPOSTA {idx + 1} (Página {page}):\n"
                structured_data_text += f"    TEXTO DO HIPERLINK: {text}\n"
                structured_data_text += f"    URI: {uri}\n\n"
        structured_data_text += "\n"
    else: # session_year >= 2020
        structured_data_text = "STRUCTURED PROPOSAL DATA EXTRACTED FROM PDF:\n\n"
        if hyperlink_table_pairs:
            has_data = True
            structured_data_text += "GRUPOS DE PROPOSTAS (HIPERLINKS) COM TABELAS DE VOTAÇÃO ASSOCIADAS:\n"
            for idx, pair in enumerate(hyperlink_table_pairs):
                structured_data_text += f"  GRUPO {idx + 1} (Página {pair.get('page_num', 'N/A')}):\n"
                structured_data_text += "    HIPERLINKS NESTE GRUPO:\n"
                for h_idx, hyperlink in enumerate(pair['hyperlinks']):
                    text = hyperlink.get('text', 'N/A')
                    uri = hyperlink.get('uri', 'N/A')
                    structured_data_text += f"      - HIPERLINK {h_idx + 1}: TEXTO='{text}', URI='{uri}'\n"
                # For brevity, we might not include the full table data in the prompt text
                # but indicate its presence. The LLM prompt will refer to this structure.
                structured_data_text += "    TABELA DE VOTAÇÃO COMPARTILHADA POR ESTE GRUPO (Presente nos dados, não detalhada aqui para brevidade)\n\n"
        
        if unpaired_links:
            has_data = True
            structured_data_text += "PROPOSTAS SEM TABELAS DE VOTAÇÃO INDIVIDUAIS (OU NÃO PAREADAS):\n"
            for idx, link_info in enumerate(unpaired_links):
                text = link_info.get('text', 'N/A')
                uri = link_info.get('uri', 'N/A')
                page = link_info.get('page_num', 'N/A')
                structured_data_text += f"  PROPOSTA NÃO PAREADA {idx + 1} (Página {page}):\n"
                structured_data_text += f"    TEXTO DA PROPOSTA: {text}\n"
                structured_data_text += f"    URI: {uri}\n\n"
    
    if not has_data:
        # print("format_structured_data_for_llm: No data to format.")
        return "NO DATA EXTRACTED FROM PDF."
        
    return structured_data_text

def create_structured_data_prompt(structured_data_text, session_year, session_date_str):
    """Create the LLM prompt for structured data, accommodating different PDF structures based on year."""
    
    if session_year < 2020:
        # Determine legislative period for MP counts
        mp_counts_info = ""
        try:
            session_dt = datetime.strptime(session_date_str, '%Y-%m-%d')
            leg_XIII_end_date = datetime(2019, 10, 25) # Includes this date
            # leg_XIV_start_date = datetime(2019, 10, 26) # Not strictly needed for pre-2020 logic here as year is < 2019 or it's 2019 before this date

            if session_year < 2019 or (session_year == 2019 and session_dt <= leg_XIII_end_date):
                # Legislatura XIII (26 Nov 2015 - 25 Out 2019)
                mp_counts_info = """
Constituição do Parlamento (Legislatura XIII: aprox. Nov 2015 - Out 2019):
- PPD/PSD.CDS-PP: 107 (Coligação PPD/PSD e CDS-PP)
- PS: 86
- BE: 19
- PCP-PEV: 17 (Coligação PCP e PEV. Se PCP e PEV votarem separadamente, os seus votos são contabilizados dentro deste total.)
- PAN: 1
Total aproximado: 230 deputados.
"""
            # Note: The second MP count block (XIV Leg) starts Oct 26, 2019.
            # Since this entire block is for session_year < 2020, any 2019 date will fall into XIII Leg for this specific pre-2020 logic.
            # For simplicity and correctness given `session_year < 2020`, we primarily expect XIII Leg counts.

        except ValueError:
            # print(f"Warning: Could not parse session_date_str '{session_date_str}' to determine precise MP counts. Using general pre-2020 prompt.")
            # Fallback if date parsing fails, though it's less ideal
            pass # mp_counts_info remains empty or could have a generic statement

        prompt = f"""Você está analisando um registo de votações parlamentares portuguesas de um período ANTERIOR A 2020.
Nestes PDFs mais antigos, as tabelas detalhadas de votação por partido geralmente NÃO estão presentes.
Em vez disso, o texto descreve quais partidos votaram a favor, contra ou se abstiveram. Exemplo:
"Favor – BE, PCP, PEV e PAN"
"Contra – PSD, PS"
"Abstenção – CDS-PP"
"Aprovado" (indica o resultado final da votação)

{mp_counts_info}
A sua tarefa é extrair as propostas (hiperlinks) e, para cada uma, determinar os detalhes da votação com base no texto fornecido e na constituição parlamentar acima.

Dados extraídos do PDF (lista de propostas/hiperlinks):
{structured_data_text}

Com base nestes dados, crie um array JSON onde cada elemento representa UMA proposta (hiperlink) que foi votada.
Para cada proposta, extraia:
1.  'proposal_name': O identificador da proposta a partir do texto do hiperlink (ex: "Projeto de Lei 404/XIII/1"). O Identificador NUNCA será "Texto Final" ou similar, apesar do hyperlink poder ter esse texto.
2.  'proposal_link': O URI/hiperlink para esta proposta.
3.  'voting_summary': O detalhamento da votação por partido.
    - Analise o texto associado à proposta (não fornecido diretamente aqui, mas você deve inferir dos padrões de votação descritos no documento original, como "Favor - PS, BE").
    - Use o formato: {{"NomeDoPartido": {{"Favor": X, "Contra": Y, "Abstenção": Z, "Não Votaram": W, "TotalDeputados": Total}}}}
    - Para cada partido mencionado como "Favor", "Contra", ou "Abstenção", assuma que TODOS os deputados desse partido votaram dessa forma. Use os totais de deputados da constituição parlamentar fornecida.
    - Se um partido não for mencionado para uma votação específica, assuma "Não Votaram" como o total de seus deputados e 0 para Favor/Contra/Abstenção para esse partido.
    - Para a coligação "PCP-PEV", se "PCP" e "PEV" forem listados separadamente no texto da votação, distribua os votos e deputados proporcionalmente ou conforme o bom senso se os números exatos não estiverem disponíveis (ex: se PCP-PEV tem 17, e PCP vota Favor, PEV vota Contra, atribua os deputados de cada um). Se apenas "PCP-PEV" for listado, atribua todos os 17 a essa entrada.
4.  'approval_status': Um inteiro, 1 se a proposta foi aprovada (ex: texto "Aprovado"), 0 se foi rejeitada (ex: texto "Rejeitado"). Se não estiver claro, defina como nulo.

Notas importantes:
- Filtre quaisquer hiperlinks que claramente não sejam propostas votadas.
- Se o texto indicar aprovação "por unanimidade", reflita isso no 'voting_summary' com todos os partidos em "Favor" e 'approval_status' como 1.
- Se não conseguir determinar as informações de votação para uma proposta, ainda a inclua com 'proposal_name' e 'proposal_link', mas defina 'voting_summary' e 'approval_status' como nulos.

Retorne apenas um array JSON válido. Cada objeto no array corresponde a um hiperlink/proposta.
Exemplo de formato de 'voting_summary' para um partido (PS com 86 deputados, votou Favor):
"PS": {{"Favor": 86, "Contra": 0, "Abstenção": 0, "Não Votaram": 0, "TotalDeputados": 86}}
"""
    else: # session_year >= 2020 (existing prompt)
        prompt = f"""Você está analisando um registro de votações parlamentares portuguesas. Eu já extraí dados estruturados de propostas do PDF. Estes dados consistem em:
1. Grupos de propostas: Cada grupo contém um ou mais hiperlinks (propostas) que *aparentam estar* associados a uma única tabela de votação encontrada após eles na mesma página. **A lista de hiperlinks fornecida para cada "grupo" é uma extração de melhor esforço de links encontrados textualmente acima de uma tabela. É possível que nem todos os hiperlinks listados sejam relevantes para essa tabela específica, e alguns podem não estar relacionados ou ser de contextos diferentes. Sua tarefa inclui discernir as propostas reais relacionadas à tabela a partir desta lista.**
2. Propostas não pareadas: Estes são hiperlinks que não tinham uma tabela imediatamente a seguir.

{structured_data_text}

Com base nestes dados estruturados, crie um array JSON onde cada elemento representa UMA proposta (hiperlink) que foi votada.
**A associação de hiperlinks a tabelas é uma tentativa baseada na proximidade no documento. Nem todos os hiperlinks listados acima de uma tabela pertencem necessariamente a essa votação; alguns podem ser de outros contextos. O modelo deve analisar criticamente para determinar a relevância.**

- **Para "GRUPOS" de hiperlinks que parecem compartilhar uma única tabela (indicado como "TABELA DE VOTAÇÃO COMPARTILHADA POR ESTE GRUPO"):**
    - **Analise cuidadosamente cada hiperlink no grupo. É possível que múltiplos hiperlinks sejam propostas válidas que foram votadas em bloco, usando a mesma tabela de resultados.**
    - **Se este for o caso, você DEVE criar um objeto JSON separado para CADA UMA dessas propostas (hiperlinks) válidas. Cada um desses objetos JSON deve conter os detalhes da votação da tabela compartilhada.** Não agrupe várias propostas em um único objeto JSON nem ignore propostas válidas dentro do grupo. Filtre quaisquer hiperlinks que claramente não sejam propostas votadas (ex: links para páginas genéricas, documentos suplementares não votados).
- Para propostas não pareadas (listadas sob "PROPOSTAS SEM TABELAS DE VOTAÇÃO INDIVIDUAIS"), tente inferir os detalhes da votação conforme descrito abaixo.

Para cada proposta (hiperlink), extraia:

1. 'proposal_name': O identificador da proposta a partir do texto do hiperlink (por exemplo, "Projeto de Lei 404/XVI/1", "Proposta de Lei 39/XVI/1"). Isso vem do 'TEXTO' do hiperlink (para propostas agrupadas) ou 'TEXTO DA PROPOSTA' (para propostas não pareadas). O Identificador NUNCA será "Texto Final" ou similar, apesar do hyperlink poder ter esse texto.
2. 'proposal_link': O URI/hiperlink para esta proposta. Isso vem do 'URI' do hiperlink.
3. 'voting_summary': O detalhamento da votação por partido.
    - Para propostas em um grupo com uma tabela compartilhada: Analise a tabela COMPARTILHADA para extrair as contagens de votos para cada partido (PS, PSD, CH, IL, PCP, BE, PAN, L, etc.)
    - Para propostas não pareadas: Se a proposta aparecer na seção "PROPOSTAS SEM TABELAS DE VOTAÇÃO INDIVIDUAIS", verifique se há algum indicador de texto no documento original (não fornecido aqui, então infira se possível a partir do contexto ou padrões comuns como aprovação unânime para certos tipos de propostas) sugerindo aprovação unânime ou votação em grupo. Se não houver informação, defina como nulo.
4. 'approval_status': Um inteiro, 1 se a proposta foi aprovada, 0 se foi rejeitada. Se não estiver claro, defina como nulo. Isso é derivado do 'voting_summary'.

Para o formato de voting_summary:
- Se houver uma tabela de votação: Analise a tabela para extrair as contagens de votos para cada partido.
- Use o formato: {{"NomeDoPartido": {{"Favor": X, "Contra": Y, "Abstenção": Z, "Não Votaram": W, "TotalDeputados": Total}}}}
- Se a tabela usar marcas 'X': A marca 'X' indica que todos os MPs daquele partido votaram daquela maneira. Use o número total mostrado para aquele partido, se disponível, caso contrário, infira com base nos tamanhos típicos dos partidos, se necessário (menos ideal).
- Se não houver tabela individual, mas for provavelmente unânime: Indique a votação unânime com as distribuições de partido apropriadas, se puder inferi-las, ou marque como unânime.

Notas importantes:
- Alguns dos hiperlinks podem não ser propostas, mas sim guias suplementares ou outros documentos. Normalmente, o primeiro hiperlink que aparece em um determinado parágrafo é a proposta principal, e pode não estar sempre vinculado ao identificador da proposta, às vezes o texto do hiperlink é apenas um genérico "Texto Final". Filtre itens não-proposta se identificáveis.
- Algumas propostas podem ser aprovadas "por unanimidade" - estas ainda devem ser incluídas com o resumo da votação indicando aprovação unânime e status de aprovação como 1.
- Múltiplas propostas podem compartilhar o mesmo resultado de votação se foram votadas juntas. **Conforme instruído acima, crie um objeto JSON separado para cada proposta nestes casos.**
- Sempre forneça contagens numéricas no resumo da votação, não apenas marcas 'X'.

Retorne apenas um array JSON válido. Cada objeto no array corresponde a um hiperlink/proposta.
Se você não conseguir determinar as informações de votação para uma proposta, ainda a inclua com seu 'proposal_name' e 'proposal_link', mas defina 'voting_summary' como nulo e 'approval_status' como nulo.

Formato de exemplo (ilustrando um grupo de duas propostas compartilhando uma tabela, e uma proposta não pareada):
[
  {{ // Do grupo, primeiro hiperlink (assumindo que é uma proposta válida relacionada à tabela)
    "proposal_name": "Projeto de Lei 123/XV/2",
    "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=XXXXX",
    "voting_summary": {{ // Derivado da tabela compartilhada
      "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
      "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
    }},
    "approval_status": 1
  }},
  {{ // Do mesmo grupo, segundo hiperlink (assumindo que é outra proposta válida relacionada à mesma tabela)
    "proposal_name": "Alteração ao Projeto de Lei 123/XV/2",
    "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=YYYYY",
    "voting_summary": {{ // Derivado DA MESMA tabela compartilhada que acima
      "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
      "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
    }},
    "approval_status": 1
  }},
  {{ // Uma proposta não pareada
    "proposal_name": "Voto de Pesar XYZ",
    "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=ZZZZZ",
    "voting_summary": null, // Ou inferido se unânime, por exemplo, {{"Unânime": {{"Favor": 200, ...}}}}
    "approval_status": null // Ou inferido, por exemplo, 1 se aprovação unânime
  }}
]
"""
    return prompt

def validate_llm_proposals_response(extracted_data):
    """Validate the LLM response and return valid proposals."""
    valid_proposals = []
    if not isinstance(extracted_data, list): 
        print(f"Warning: LLM response was not a list, but {type(extracted_data)}. Data: {str(extracted_data)[:200]}")
        return [] 

    for item in extracted_data:
        if isinstance(item, dict) and 'proposal_name' in item and item['proposal_name'] is not None: 
            valid_proposals.append(item)
        else:
            print(f"Warning: LLM returned an invalid item structure or missing proposal_name: {item}")
    return valid_proposals

# --- Script 3: Get Proposals (Scrape Proposal Details & Download Document) ---
def fetch_proposal_details_and_download_doc(proposal_page_url, download_dir):
    """
    Fetches author info and document link from proposal_page_url.
    Downloads the document if it's a PDF or DOCX.
    Updates document_info with download status and local path.
    """
    authors_list = []
    # Initialize document_info with more robust default states
    document_info = {
        'link': None, 
        'type': None, 
        'local_path': None, 
        'download_status': 'Not Attempted', 
        'error': None
    }
    scrape_status = 'Pending' # Overall status for scraping this proposal page
    html_content = None

    print(f"Fetching proposal details from: {proposal_page_url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(proposal_page_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        html_content = response.text
        scrape_status = 'Success (HTML Fetched)'
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch HTML from {proposal_page_url}: {e}"
        print(error_msg)
        document_info['error'] = error_msg # Store error related to fetching the page itself
        scrape_status = f'Error: {error_msg}'
        # Return early if we can't even get the proposal page
        return {
            'authors_json': json.dumps([]) if not authors_list else json.dumps(authors_list), # Ensure it's always valid JSON string
            'document_info': document_info,
            'scrape_status': scrape_status,
            'error': document_info['error'] 
        }

    soup = BeautifulSoup(html_content, 'lxml') 
    base_url = f"{urlparse(proposal_page_url).scheme}://{urlparse(proposal_page_url).netloc}"

    # --- Extract Authors ---
    autoria_heading = soup.find(lambda tag: tag.name == "div" and "Autoria" in tag.get_text(strip=True) and "Titulo-Cinzento" in tag.get("class", []))
    if autoria_heading:
        authors_div = autoria_heading.find_next_sibling('div')
        if authors_div:
            author_tags = authors_div.find_all('a') # Assuming authors are linked
            if not author_tags: # Fallback if no links, try list items or paragraphs
                author_tags = authors_div.find_all('li') or authors_div.find_all('p')
            
            for author_tag in author_tags:
                author_name = author_tag.get_text(strip=True)
                if author_name:
                    authors_list.append(author_name)
    
    authors_json = json.dumps(authors_list) if authors_list else json.dumps([]) # Ensure valid JSON string

    # --- Find Document Link (PDF or DOCX) ---
    # More specific search for document links, prioritizing PDF
    doc_search_priority = [
        ('PDF', [lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoPDF')), 
                   lambda s: s.find('a', string=lambda t: t and '[formato PDF]' in t.strip().lower()), 
                   lambda s: next((tag for tag in s.find_all('a', href=True) if '.pdf' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['pdf', 'documento', 'ficheiro', 'texto integral', 'texto final', 'proposta de lei', 'projeto de lei'])), None)]),
        ('DOCX', [lambda s: next((tag for tag in s.find_all('a', href=True) if '.docx' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['docx', 'documento', 'word', 'proposta de lei', 'projeto de lei'])), None)]),
        # Fallback for .doc if others fail
        ('DOC', [lambda s: next((tag for tag in s.find_all('a', href=True) if '.doc' in tag.get('href','').lower() and not '.docx' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['doc', 'documento', 'word', 'proposta de lei', 'projeto de lei'])), None)])
    ]

    found_doc_link_tag = None
    doc_url_to_download = None

    for doc_type, search_methods in doc_search_priority:
        if found_doc_link_tag: break
        for method in search_methods:
            try_tag = method(soup)
            if try_tag and try_tag.get('href'):
                found_doc_link_tag = try_tag
                document_info['type'] = doc_type
                doc_url_to_download = urljoin(base_url, found_doc_link_tag.get('href'))
                document_info['link'] = doc_url_to_download
                print(f"Found potential document link ({doc_type}): {doc_url_to_download}")
                break
    
    if not doc_url_to_download:
        msg = "No downloadable document link (PDF/DOCX/DOC) found on proposal page."
        print(msg)
        document_info['download_status'] = 'Skipped - No Document Link'
        document_info['error'] = msg # This is not a scrape error, but info
        scrape_status = 'Success (No Document Link Found)' # Page scraped, but no doc
    else:
        # Generate filename based on proposal_page_url (e.g., from BID) and doc_type
        parsed_url = urlparse(proposal_page_url)
        query_params = parse_qs(parsed_url.query)
        bid = query_params.get('BID', [None])[0]
        doc_id_for_filename = bid if bid else hashlib.md5(doc_url_to_download.encode()).hexdigest()[:10]
        
        file_extension = ".pdf" if document_info['type'] == 'PDF' else ".docx" if document_info['type'] == 'DOCX' else ".doc"
        # Ensure filename is safe
        safe_doc_id = re.sub(r'[^a-zA-Z0-9_\-]', '', str(doc_id_for_filename))
        local_filename = f"proposal_{safe_doc_id}_formato_{document_info['type']}{file_extension}"
        local_path = os.path.join(download_dir, local_filename)
        document_info['local_path'] = local_path

        # Attempt to download the document
        # download_file now returns a single status string: "Success" or an error message.
        download_status_message = download_file(doc_url_to_download, local_path, is_pdf=(document_info['type'] == 'PDF'))

        if download_status_message == "Success":
            document_info['download_status'] = 'Success'
            scrape_status = 'Success (Document Downloaded)' # Overall success for this proposal
            document_info['error'] = None # Clear any previous non-critical error like "No document link found"
        else:
            document_info['download_status'] = download_status_message # Store the error message from download_file
            document_info['error'] = download_status_message # Also store it as the main error for this stage
            scrape_status = f'Error: Document Download Failed - {download_status_message}'
            # Do not overwrite local_path if download failed, it might be useful for debugging
            # or if a partial file was created (though download_file tries to avoid empty files)
        
    return {
        'authors_json': authors_json,
        'document_info': document_info,
        'scrape_status': scrape_status, # This reflects the overall status of this function call
        'error': document_info['error'] # This is the most relevant error from this function call
    }

# --- Script 4: Proposal Summary (Summarize Proposal Document with LLM) ---
def summarize_proposal_text(proposal_document_path):
    prompt = """Provide this answer in Portuguese from Portugal: This is a government proposal that was voted on in the Portuguese Parliament in Portugal and so is full of legal language. Analyze this document and provide a structured JSON response with the following fields:

1. "general_summary": A general summary of the proposal, avoiding legalese and using normal vocabulary
2. "critical_analysis": Think critically about the document and point out inconsistencies if there are any, and if not show how the implementation details align with the goal
3. "fiscal_impact": An educated estimate if the proposal will increase or decrease government spending and increase or decrease government revenue as well, and what the net effect may be
4. "colloquial_summary": Another summary, but in more colloquial language
5. "categories": An array of one or more categories that this proposal fits into. Choose from the following categories, only output it's index in an array format, do not output the category name itself:
   0 - "Saude e Cuidados Sociais"
   1 - "Educacao e Competências"
   2 - "Defesa e Segurança Nacional"
   3 - "Justica, Lei e Ordem"
   4 - "Economia e Financas"
   5 - "Bem-Estar e Seguranca Social"
   6 - "Ambiente, Agricultura e Pescas"
   7 - "Energia e Clima"
   8 - "Transportes e Infraestruturas"
   9 - "Habitacao, Comunidades e Administracao Local"
   10 - "Negocios Estrangeiros e Cooperacao Internacional"
   11 - "Ciencia, Tecnologia e Digital"
6. "short_title": A concise title for the proposal, maximum 10 words.
7. "proposing_party": The political party or entity that proposed this initiative (e.g., "PCP", "PS", "PSD", "Governo"). Extract this from the document text, often found near the proposal title or number. If not clearly identifiable, set to null.

Return only a valid JSON object with these 7 fields. If the proposal fits multiple categories, include all relevant ones in the "categories" array.

Example format:
{
  "general_summary": "...",
  "critical_analysis": "...",
  "fiscal_impact": "...",
  "colloquial_summary": "...",
  "categories": [4, 5],
  "short_title": "Reforma do Sistema de Pensões",
  "proposing_party": "PCP"
}
"""
    summary_data, error = call_gemini_api(prompt, document_path=proposal_document_path, expect_json=True)
    if error:
        return None, f"LLM API call failed for summary: {error}"
    
    if not isinstance(summary_data, dict):
        return None, f"LLM did not return a JSON object as expected. Got: {type(summary_data)}"
    
    required_fields = ['general_summary', 'critical_analysis', 'fiscal_impact', 'colloquial_summary', 'categories', 'short_title', 'proposing_party']
    for field in required_fields:
        if field not in summary_data:
            if field == 'proposing_party' and summary_data.get(field) is None:
                summary_data[field] = None 
            else:
                return None, f"Missing required field '{field}' in LLM response: {summary_data}"
    
    if not isinstance(summary_data.get('categories'), list): 
        if isinstance(summary_data.get('categories'), int):
            summary_data['categories'] = [summary_data['categories']]
        elif summary_data.get('categories') is None:
             summary_data['categories'] = [] 
        else:
            return None, f"Field 'categories' should be a list, got: {type(summary_data.get('categories'))}"
    
    summary_data['categories'] = json.dumps(summary_data['categories'])
    
    return summary_data, None

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
        print(f"Error generating session PDF filename for {session_pdf_url}: {e}. Using fallback.")
        url_hash = hashlib.md5(session_pdf_url.encode()).hexdigest()[:10]
        return f"session_{session_year_param}_{url_hash}_fallback.pdf"


# --- Main Pipeline Orchestrator ---
def run_pipeline(start_year=None, end_year=None, max_sessions_to_process=None):
    if not GEMINI_API_KEY:
        print("Critical Error: GEMINI_API_KEY is not set. The pipeline cannot run LLM-dependent stages.")
        return

    init_directories()
    df = load_or_initialize_dataframe()

    processed_dates_in_df = set()
    last_date_in_df_for_reprocessing_check = None

    if not df.empty and 'session_date' in df.columns:
        # Attempt to parse session_date to datetime objects for reliable comparison and duplicate check
        try:
            # Convert to datetime, coercing errors to NaT, then drop NaT if any for safety
            df['session_date_dt'] = pd.to_datetime(df['session_date'], errors='coerce')
            processed_dates_in_df = set(df.dropna(subset=['session_date_dt'])['session_date_dt'].dt.strftime('%Y-%m-%d'))
            
            # For reprocessing check, find the latest date string from the original column
            # after ensuring it's a valid date by checking corresponding 'session_date_dt'
            valid_dates_df = df.dropna(subset=['session_date_dt'])
            if not valid_dates_df.empty:
                last_date_in_df_for_reprocessing_check = valid_dates_df.sort_values(by='session_date_dt', ascending=False).iloc[0]['session_date']
            # print(f"Found {len(processed_dates_in_df)} processed dates in DataFrame. Last date for reprocessing check: {last_date_in_df_for_reprocessing_check}")
        except Exception as e:
            # print(f"Error processing session_date column for duplicate check: {e}. Proceeding without date filtering.")
            processed_dates_in_df = set() # Reset to avoid partial/incorrect filtering
            last_date_in_df_for_reprocessing_check = None
        finally:
            if 'session_date_dt' in df.columns: # Clean up temporary column
                df.drop(columns=['session_date_dt'], inplace=True)


    scraper = ParliamentPDFScraper()
    current_year = datetime.now().year
    _start_year = start_year if start_year else current_year - 5 
    _end_year = end_year if end_year else current_year
    
    print(f"--- Stage 1: Fetching all session PDF links from website for years {_start_year}-{_end_year} ---")
    all_session_pdf_infos_from_web = scraper.scrape_years(start_year=_start_year, end_year=_end_year)
    print(f"Found {len(all_session_pdf_infos_from_web)} potential session PDF links from web.")

    TERMINAL_SUCCESS_STATUSES = {
        'Success', 
        'Completed (No Propostas)', 
        'Completed (No Proposal Doc to Summarize)', 
        'Completed (No Gov Link for Details)'
    }
    
    sessions_to_process_infos = []
    if not df.empty and 'session_date' in df.columns and processed_dates_in_df:
        for info in all_session_pdf_infos_from_web:
            current_web_session_date = info.get('date') # Expected 'YYYY-MM-DD'

            if pd.isna(current_web_session_date):
                # If date from web is missing, process it to be safe or log error
                print(f"Warning: Session info from web has no date: {info['url']}. Adding for processing.")
                sessions_to_process_infos.append(info)
                continue

            if current_web_session_date in processed_dates_in_df:
                if current_web_session_date == last_date_in_df_for_reprocessing_check:
                    # This date matches the last entry's date in CSV, mark for re-processing.
                    print(f"Session date {current_web_session_date} matches last CSV entry date. Adding for re-processing.")
                    sessions_to_process_infos.append(info)
                else:
                    # This date is in CSV and is not the last entry's date, so skip.
                    # print(f"Skipping already processed session date: {current_web_session_date}")
                    pass 
            else:
                # This date is not in CSV, so process.
                sessions_to_process_infos.append(info)
        print(f"Filtered to {len(sessions_to_process_infos)} sessions after considering processed dates.")
    else: # DataFrame is empty or has no session_date column or no processed dates
        sessions_to_process_infos = all_session_pdf_infos_from_web
        print("Processing all sessions found from web (CSV empty or no relevant dates).")


    # Sort sessions: prioritize reprocessing the last known date, then by date.
    if last_date_in_df_for_reprocessing_check:
        sessions_to_process_infos.sort(key=lambda x: (str(x.get('date', '1900-01-01')) != str(last_date_in_df_for_reprocessing_check), str(x.get('date', '1900-01-01')), x['url']))
    else:
        sessions_to_process_infos.sort(key=lambda x: (str(x.get('date', '1900-01-01')), x['url']))

    
    print(f"Total sessions to iterate through after filtering and sorting: {len(sessions_to_process_infos)}")

    processed_sessions_count = 0
    for session_info in sessions_to_process_infos:
        # session_info is expected to be a dict like {'url': '...', 'date': 'YYYY-MM-DD', 'year': YYYY}
        session_pdf_url = session_info['url']
        session_date_str = session_info['date'] # This is 'YYYY-MM-DD'
        session_year_from_info = session_info['year'] # This is YYYY (integer)

        # print(f"\nProcessing session: URL='{session_pdf_url}', Date='{session_date_str}', Year='{session_year_from_info}'")

        # Generate filename based on URL and explicit year from session_info
        session_pdf_filename = generate_session_pdf_filename(session_pdf_url, session_year_from_info)
        if not session_pdf_filename:
            # print(f"Could not generate filename for URL {session_pdf_url}, skipping.")
            # Potentially update df with an error for this URL if it was already there
            continue 
        
        session_pdf_full_path = os.path.join(SESSION_PDF_DIR, session_pdf_filename)

        # Check if this specific session (by date) already exists and has a terminal success status
        # This check is refined to ensure we are looking at the correct row if multiple entries for a date exist (though ideally not)
        existing_rows = df[(df['session_pdf_url'] == session_pdf_url) & (df['session_date'] == session_date_str)]
        
        # If we want to re-process the very last date found in the CSV, we adjust the skip logic
        should_reprocess_this_specific_date = (session_date_str == last_date_in_df_for_reprocessing_check)
        
        if not existing_rows.empty and not should_reprocess_this_specific_date:
            # If exists and not the one to reprocess, check its status
            row_status = existing_rows.iloc[0]['overall_status']
            if row_status in TERMINAL_SUCCESS_STATUSES:
                # print(f"Skipping {session_pdf_url} (Date: {session_date_str}) as it's already processed with status: {row_status}.")
                continue
            # else:
                # print(f"Reprocessing {session_pdf_url} (Date: {session_date_str}) due to non-terminal status: {row_status}")
        elif should_reprocess_this_specific_date:
            # print(f"Flagged for reprocessing (or initial processing if new): {session_pdf_url} (Date: {session_date_str}) as it's the last known date or matches reprocessing criteria.")
            # If it exists, clear relevant fields before reprocessing
            if not existing_rows.empty:
                idx_to_clear = existing_rows.index
                # print(f"Clearing fields for reprocessing for index {idx_to_clear} corresponding to date {session_date_str}")
                columns_to_reset = [
                    'session_pdf_text_path', 'session_pdf_download_status',
                    'proposal_name_from_session', 'proposal_gov_link', 'voting_details_json', 'session_parse_status',
                    'proposal_authors_json', 'proposal_document_url', 'proposal_document_type', 
                    'proposal_document_local_path', 'proposal_doc_download_status', 'proposal_details_scrape_status',
                    'proposal_summary_general', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 
                    'proposal_summary_colloquial', 'proposal_category', 'proposal_summarize_status',
                    'proposal_approval_status', 'proposal_short_title', 'proposal_proposing_party', 
                    'overall_status', 'last_error_message'
                ]
                for col in columns_to_reset:
                    if col in df.columns:
                        df.loc[idx_to_clear, col] = pd.NA # Use pd.NA for missing values
                df.loc[idx_to_clear, 'last_processed_timestamp'] = datetime.now().isoformat()


        # --- Stage 1.1: Download individual session PDF ---
        # Update DataFrame immediately with info about this session being processed
        # Find row by URL and date, or add new row
        row_index = df[(df['session_pdf_url'] == session_pdf_url) & (df['session_date'] == session_date_str)].index
        if row_index.empty:
            new_row_data = {col: pd.NA for col in df.columns} # Initialize with pd.NA
            new_row_data.update({
                'session_pdf_url': session_pdf_url,
                'session_year': session_year_from_info, # Use year from session_info
                'session_date': session_date_str,       # Use date from session_info
                'overall_status': 'Processing Stage 1 (Download)',
                'last_processed_timestamp': datetime.now().isoformat()
            })
            # df = df.append(new_row_data, ignore_index=True) # .append is deprecated
            df = pd.concat([df, pd.DataFrame([new_row_data])], ignore_index=True)
            row_index = df.index[-1] # Get the index of the newly added row
        else:
            row_index = row_index[0] # Take the first match if multiple (should ideally be unique by url+date)
            df.loc[row_index, 'overall_status'] = 'Reprocessing Stage 1 (Download)'
            df.loc[row_index, 'last_processed_timestamp'] = datetime.now().isoformat()
            # Ensure year and date are correctly set from session_info, especially if reprocessing
            df.loc[row_index, 'session_year'] = session_year_from_info
            df.loc[row_index, 'session_date'] = session_date_str


        # download_status_bool, download_message = download_file(session_pdf_url, session_pdf_full_path) # OLD call from previous modification
        download_status_string = download_file(session_pdf_url, session_pdf_full_path) # NEW call

        # df.loc[row_index, 'session_pdf_download_status'] = "Success" if download_status_bool else f"Failed: {download_message}" # OLD logic from previous modification
        df.loc[row_index, 'session_pdf_download_status'] = download_status_string # NEW logic - store the string directly

        # if not download_status_bool: # OLD logic from previous modification
        if download_status_string != "Success": # NEW logic
            df.loc[row_index, 'overall_status'] = 'Error - Download Failed'
            # df.loc[row_index, 'last_error_message'] = f"Download failed: {download_message}" # OLD logic from previous modification
            df.loc[row_index, 'last_error_message'] = download_status_string # NEW logic - store the full error string from download_file
            save_dataframe(df)
            continue
        
        # If download was "Success":
        df.loc[row_index, 'last_error_message'] = pd.NA # Clear any previous error message
        df.loc[row_index, 'overall_status'] = 'Processing Stage 2 (Parse Session PDF)' # Update status for next stage
        save_dataframe(df) # Save after successful download and status update

        # --- Stage 2: Parse Session PDF (Extract text or use LLM for structure) ---
        # Pass session_year_from_info and session_date_str
        proposals_data, error_parsing_session = extract_votes_from_session_pdf_text(session_pdf_full_path, session_year_from_info, session_date_str)

        if error_parsing_session:
            df.loc[row_index, 'session_parse_status'] = 'Error'
            df.loc[row_index, 'last_error_message'] = f"Session PDF parsing error: {error_parsing_session}"
            df.loc[row_index, 'overall_status'] = 'Error - Session Parse Failed'
            save_dataframe(df)
            continue # Skip to next session if base parsing fails
        
        if not proposals_data: # No proposals found, but no direct error string
            df.loc[row_index, 'session_parse_status'] = 'Success (No Propostas Found)'
            df.loc[row_index, 'overall_status'] = 'Completed (No Propostas)'
            df.loc[row_index, 'last_error_message'] = pd.NA # Clear previous errors
            save_dataframe(df)
            processed_sessions_count += 1
            if max_sessions_to_process and processed_sessions_count >= max_sessions_to_process: break
            continue # Successfully processed this session, but no proposals to detail further

        # If proposals_data is not empty, it means Stage 2 was successful in finding items
        df.loc[row_index, 'session_parse_status'] = 'Success'
        df.loc[row_index, 'last_error_message'] = pd.NA # Clear previous errors
        # print(f"Successfully parsed {len(proposals_data)} proposal items from {session_pdf_url}")

        # We need to add/update rows in the DataFrame for each proposal found
        # This session (identified by row_index) is the "parent"
        # Each proposal will be a new row or update an existing one if we can match it.
        # For now, let's assume each run for a session PDF generates new proposal entries,
        # or we need a robust way to match/update proposals if they were partially processed before.

        # To simplify, we'll create new rows for proposals from this session,
        # copying parent session info. If re-running, old proposal rows from this session
        # might become "orphaned" or need explicit deletion/update logic.
        # Current approach: if parent session is reprocessed, its old proposal rows are not touched.
        # New proposal rows are added. This can lead to duplicates if not handled carefully in analysis.

        # A better approach for reprocessing: Delete old proposal rows linked to this session PDF before adding new ones.
        # For now, let's find existing proposal rows that came from THIS session PDF and THIS session_date
        # and update them if the number matches, or delete/add if numbers differ.
        # This is complex. Simpler: If reprocessing, mark old proposals as 'stale' or delete.
        
        # If this session is being reprocessed (was already in df and not skipped),
        # we should clear out its old proposals before adding new ones.
        if should_reprocess_this_specific_date or (not existing_rows.empty and existing_rows.iloc[0]['overall_status'] not in TERMINAL_SUCCESS_STATUSES) :
            # print(f"Clearing old proposal entries for reprocessed session: {session_pdf_url}")
            # This condition means we are reprocessing. Find rows that are 'children' of this session.
            # A 'child' proposal row would have the same 'session_pdf_url' and 'session_date'
            # but also conter proposal-specific details.
            # The main DataFrame `df` might have one row for the session summary itself,
            # and then multiple rows for each proposal from that session.
            # The current structure seems to be one row per proposal, with session info duplicated.
            
            # Identify rows that are proposals from this specific session PDF
            # These rows would have session_pdf_url and session_date matching, AND proposal_name is not NA
            indices_to_delete = df[
                (df['session_pdf_url'] == session_pdf_url) & \
                (df['session_date'] == session_date_str) & \
                (df['proposal_name_from_session'].notna()) # This signifies it's a proposal row, not the parent session summary row
            ].index
            
            if not indices_to_delete.empty:
                # print(f"Deleting {len(indices_to_delete)} old proposal entries linked to {session_pdf_url} before adding new ones.")
                df.drop(indices_to_delete, inplace=True)
                df.reset_index(drop=True, inplace=True) # Reset index after drop
                # The original `row_index` for the session summary might be invalidated by this. Re-fetch it.
                # This is tricky. The `row_index` currently points to the "session summary" row which might not have proposal_name.
                # The logic here is that `df` contains one row per proposal.
                # So, if a session PDF yields 3 proposals, we need 3 rows in the CSV.

                # If `row_index` was for the *session itself* (e.g. overall status),
                # now we need to transform/expand it for each proposal.
                
                # Simpler model for now: The initial `df.loc[row_index]` is the *first* proposal's row.
                # Additional proposals get new rows.
                # This means if a session was 'Completed (No Propostas)' and is reprocessed to find proposals,
                # that original row needs to be updated for the first proposal.
                pass # The deletion logic above should handle clearing old proposal-specific rows.
                     # The current `row_index` should point to the "session summary" row which will be basis for new proposal rows


        # Create/Update rows for each extracted proposal
        for i, prop_data in enumerate(proposals_data):
            current_proposal_name = prop_data.get('proposal_name')
            current_proposal_link = prop_data.get('proposal_link') # This is proposal_gov_link
            
            # Determine the index for this specific proposal
            # If it's the first proposal from this session, we can try to update the existing session row (row_index)
            # Otherwise, we create a new row.
            if i == 0:
                proposal_row_index = row_index # Use the existing session row for the first proposal
                df.loc[proposal_row_index, 'overall_status'] = 'Processing Stage 3 (Proposal Details)' # Update status
            else:
                # Create a new row for subsequent proposals, copying session-level data
                new_proposal_row_data = df.loc[row_index].to_dict() # Copy from the session summary row
                # Clear proposal-specific fields that will be newly populated
                proposal_specific_cols = [
                    'proposal_name_from_session', 'proposal_gov_link', 'voting_details_json', 
                    'proposal_authors_json', 'proposal_document_url', 'proposal_document_type', 
                    'proposal_document_local_path', 'proposal_doc_download_status', 'proposal_details_scrape_status',
                    'proposal_summary_general', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 
                    'proposal_summary_colloquial', 'proposal_category', 'proposal_summarize_status',
                    'proposal_approval_status', 'proposal_short_title', 'proposal_proposing_party',
                    'last_error_message'
                ]
                for col in proposal_specific_cols: new_proposal_row_data[col] = pd.NA
                
                new_proposal_row_data['overall_status'] = 'Processing Stage 3 (Proposal Details)'
                new_proposal_row_data['last_processed_timestamp'] = datetime.now().isoformat()
                
                # df = df.append(new_proposal_row_data, ignore_index=True) # deprecated
                df = pd.concat([df, pd.DataFrame([new_proposal_row_data])], ignore_index=True)
                proposal_row_index = df.index[-1] # Index of this new proposal row

            # Populate proposal data from LLM Stage 2
            df.loc[proposal_row_index, 'proposal_name_from_session'] = current_proposal_name
            df.loc[proposal_row_index, 'proposal_gov_link'] = current_proposal_link # This is the link to the proposal page on parlamento.pt
            df.loc[proposal_row_index, 'voting_details_json'] = json.dumps(prop_data.get('voting_summary')) if prop_data.get('voting_summary') else pd.NA
            df.loc[proposal_row_index, 'proposal_approval_status'] = prop_data.get('approval_status', pd.NA)
            df.loc[proposal_row_index, 'last_error_message'] = pd.NA # Clear previous errors for this proposal row

            # --- Stage 3: Get Proposal Details (Authors, Document Link from proposal_gov_link) ---
            if pd.isna(current_proposal_link) or not current_proposal_link:
                df.loc[proposal_row_index, 'proposal_details_scrape_status'] = 'Skipped - No Gov Link'
                df.loc[proposal_row_index, 'overall_status'] = 'Completed (No Gov Link for Details)'
                # No error message here, it's a normal skip
                save_dataframe(df)
                # For this proposal, processing ends. If it's the last proposal of the session, then session processing might end.
                # The main loop `processed_sessions_count` should only increment after ALL proposals of a session are done.
                continue # to the next proposal in proposals_data

            # print(f"Fetching details for proposal: {current_proposal_name} from {current_proposal_link}")
            details_scrape_result = fetch_proposal_details_and_download_doc(current_proposal_link, PROPOSAL_DOC_DIR)
            
            df.loc[proposal_row_index, 'proposal_authors_json'] = details_scrape_result.get('authors_json', pd.NA)
            doc_info = details_scrape_result.get('document_info', {})
            df.loc[proposal_row_index, 'proposal_document_url'] = doc_info.get('link', pd.NA)
            df.loc[proposal_row_index, 'proposal_document_type'] = doc_info.get('type', pd.NA)
            df.loc[proposal_row_index, 'proposal_document_local_path'] = doc_info.get('local_path', pd.NA)
            df.loc[proposal_row_index, 'proposal_doc_download_status'] = doc_info.get('download_status', 'Not Attempted')
            df.loc[proposal_row_index, 'proposal_details_scrape_status'] = details_scrape_result.get('scrape_status', 'Unknown')

            if details_scrape_result.get('error'):
                df.loc[proposal_row_index, 'last_error_message'] = f"Proposal details scrape/download error: {details_scrape_result['error']}"
                df.loc[proposal_row_index, 'overall_status'] = 'Error - Proposal Details Scrape Failed'
                save_dataframe(df)
                continue # To the next proposal

            df.loc[proposal_row_index, 'overall_status'] = 'Processing Stage 4 (Summarize Proposal)'
            save_dataframe(df)

            # --- Stage 4: Summarize Proposal Document (if downloaded) ---
            proposal_document_path = df.loc[proposal_row_index, 'proposal_document_local_path']
            if pd.isna(proposal_document_path) or not proposal_document_path or not os.path.exists(proposal_document_path):
                df.loc[proposal_row_index, 'proposal_summarize_status'] = 'Skipped - No Document'
                df.loc[proposal_row_index, 'overall_status'] = 'Completed (No Proposal Doc to Summarize)'
                # No error message here
                save_dataframe(df)
                continue # To the next proposal

            # print(f"Summarizing document: {proposal_document_path}")
            summary_dict, error_summarizing = summarize_proposal_text(proposal_document_path)

            if error_summarizing:
                df.loc[proposal_row_index, 'proposal_summarize_status'] = 'Error'
                df.loc[proposal_row_index, 'last_error_message'] = f"Summarization error: {error_summarizing}"
                df.loc[proposal_row_index, 'overall_status'] = 'Error - Summarization Failed'
            elif summary_dict:
                df.loc[proposal_row_index, 'proposal_summary_general'] = summary_dict.get('general_summary', pd.NA)
                df.loc[proposal_row_index, 'proposal_summary_analysis'] = summary_dict.get('critical_analysis', pd.NA)
                df.loc[proposal_row_index, 'proposal_summary_fiscal_impact'] = summary_dict.get('fiscal_impact', pd.NA)
                df.loc[proposal_row_index, 'proposal_summary_colloquial'] = summary_dict.get('colloquial_summary', pd.NA)
                df.loc[proposal_row_index, 'proposal_category'] = summary_dict.get('categories', pd.NA) # Should be JSON string
                df.loc[proposal_row_index, 'proposal_short_title'] = summary_dict.get('short_title', pd.NA)
                df.loc[proposal_row_index, 'proposal_proposing_party'] = summary_dict.get('proposing_party', pd.NA)
                df.loc[proposal_row_index, 'proposal_summarize_status'] = 'Success'
                df.loc[proposal_row_index, 'overall_status'] = 'Success' # Final success for this proposal
                df.loc[proposal_row_index, 'last_error_message'] = pd.NA # Clear error
            else: # No summary_dict and no error_summarizing means something unexpected
                df.loc[proposal_row_index, 'proposal_summarize_status'] = 'Failed - Unknown'
                df.loc[proposal_row_index, 'last_error_message'] = "Summarization returned no data and no error."
                df.loc[proposal_row_index, 'overall_status'] = 'Error - Summarization Failed (Unknown)'
            
            save_dataframe(df)
            # End of processing for one proposal within a session

        # After all proposals for the current session_info are processed (or skipped)
        processed_sessions_count += 1
        # print(f"Finished processing all proposals for session: {session_pdf_url}. Processed sessions count: {processed_sessions_count}")
        if max_sessions_to_process and processed_sessions_count >= max_sessions_to_process:
            # print(f"Reached max_sessions_to_process ({max_sessions_to_process}). Stopping.")
            break # Break from the loop over sessions_to_process_infos

    print("\\n--- Pipeline Run Finished ---")
    if not df.empty:
        print("Overall Status Counts:")
        print(df['overall_status'].value_counts(dropna=False))
    else:
        print("DataFrame is empty.")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the Parliament PDF Scraper Pipeline.")
    parser.add_argument('--year', type=int, help="Start year for scraping (default: current year - 5)", default=YEAR)
    
    args = parser.parse_args()
    YEAR = args.year
    DATAFRAME_PATH = f"data/parliament_data_{YEAR}.csv"

    run_pipeline(start_year=YEAR, end_year=YEAR, max_sessions_to_process=None)
