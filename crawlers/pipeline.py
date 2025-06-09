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
DATAFRAME_PATH = "data/parliament_data.csv"
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
    """Downloads a file from a URL to a destination path."""
    print(f"Attempting to download: {url} to {destination_path}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT, stream=True)
        response.raise_for_status()
        
        # Check content type for PDFs if expected
        if is_pdf:
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' not in content_type:
                print(f"Warning: Expected PDF, but got Content-Type: {content_type} for {url}")
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

def extract_hyperlink_table_pairs_and_unpaired_links(pdf_path):
    """
    Extracts groups of hyperlinks and their single associated table from a PDF.
    Also returns a list of hyperlinks that did not have a table immediately following them.
    A table is associated with all hyperlinks that appear directly before it
    on the same page and after any previously processed table or its associated hyperlinks.
    """
    extracted_pairs = []
    unpaired_hyperlinks = []
    doc_fitz = fitz.open(pdf_path)

    for page_num in range(len(doc_fitz)):
        page_fitz = doc_fitz[page_num]
        
        # 1. Extract hyperlinks from the current page
        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link['kind'] == fitz.LINK_URI:
                uri = link['uri']
                if ".pdf" in uri.lower(): 
                    continue
                rect = link['from']  
                link_text = page_fitz.get_text("text", clip=rect).strip()
                
                page_hyperlinks.append({
                    'text': link_text if link_text else "N/A", 
                    'uri': uri,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1), 
                    'page_num_fitz': page_num 
                })
        
        page_hyperlinks.sort(key=lambda h: h['rect'][1])

        # 2. Extract tables from the current page using tabula
        page_tables_data = []
        try:
            tables_on_page_json = tabula.read_pdf(pdf_path, 
                                                  pages=str(page_num + 1), 
                                                  output_format="json", 
                                                  multiple_tables=True, 
                                                  lattice=True, 
                                                  silent=True) 
            if not tables_on_page_json: 
                tables_on_page_json = tabula.read_pdf(pdf_path, 
                                                      pages=str(page_num + 1), 
                                                      output_format="json", 
                                                      multiple_tables=True, 
                                                      stream=True,
                                                      silent=True)
        except Exception as e:
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
                'page_num_fitz': page_num 
            })
            
        page_tables_data.sort(key=lambda t: t['top'])

        # 3. Correlate hyperlinks and tables on the page
        hyperlink_cursor = 0 
        
        for table_idx in range(len(page_tables_data)):
            table = page_tables_data[table_idx]
            table_top_y = table['top']
            
            links_for_current_table = []
            
            temp_cursor_for_this_table = hyperlink_cursor
            while temp_cursor_for_this_table < len(page_hyperlinks):
                hyperlink = page_hyperlinks[temp_cursor_for_this_table]
                hyperlink_bottom_y = hyperlink['rect'][3] 

                if hyperlink_bottom_y < table_top_y:
                    links_for_current_table.append({
                        'text': hyperlink['text'],
                        'uri': hyperlink['uri']
                    })
                    temp_cursor_for_this_table += 1
                else:
                    break 
            
            if links_for_current_table:
                extracted_pairs.append({
                    'hyperlinks': links_for_current_table, 
                    'table_data': table['dataframe'],
                    'page_num': table['page_num_fitz'] + 1 
                })
                hyperlink_cursor = temp_cursor_for_this_table 
        
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            unpaired_hyperlinks.append({
                'hyperlink_text': hyperlink['text'], 
                'uri': hyperlink['uri'],
                'page_num': hyperlink['page_num_fitz'] + 1 
            })

    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks


def extract_votes_from_session_pdf_text(session_pdf_path):
    """Enhanced voting extraction using manual PDF parsing followed by LLM processing."""
    print(f"Starting enhanced PDF parsing for: {session_pdf_path}")
    
    try:
        doc_fitz = fitz.open(session_pdf_path)
        page_count = len(doc_fitz)
        doc_fitz.close()
        print(f"PDF has {page_count} pages")
        
        if page_count > PDF_PAGE_PARTITION_SIZE:
            return process_long_pdf_in_chunks(session_pdf_path, page_count)
    except Exception as e:
        print(f"Error checking PDF page count: {e}")
    
    try:
        hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_pairs_and_unpaired_links(session_pdf_path)
        print(f"Manual parsing found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")
    except Exception as e:
        print(f"Manual PDF parsing failed: {e}. Falling back to original text extraction method.")
        return None, f"Critical failure in manual PDF parsing: {e}" 
    
    structured_data_text = format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links)
    
    if not structured_data_text.strip() or "NO DATA" in structured_data_text: 
        print("No structured data extracted from PDF to send to LLM.")
        return [], None 

    extracted_data, error = call_gemini_api(create_structured_data_prompt(structured_data_text), expect_json=True)
    if error:
        return None, f"LLM API call failed: {error}"
    if not isinstance(extracted_data, list): 
        if isinstance(extracted_data, dict) and 'proposal_name' in extracted_data:
            extracted_data = [extracted_data]
        else:
            return None, f"LLM did not return a list as expected. Got: {type(extracted_data)}"
    
    valid_proposals = validate_llm_proposals_response(extracted_data)
    
    if not valid_proposals and extracted_data: 
         return None, f"LLM returned data but no valid proposal structures found. Raw: {str(extracted_data)[:500]}"
    elif not valid_proposals and not extracted_data: 
        return [], None 

    print(f"Successfully extracted {len(valid_proposals)} proposals using enhanced parsing method")
    return valid_proposals, None

def process_long_pdf_in_chunks(session_pdf_path, page_count):
    """Process a long PDF by partitioning it into smaller chunks."""
    print(f"Processing long PDF ({page_count} pages) in chunks of {PDF_PAGE_PARTITION_SIZE} pages")
    
    all_proposals = []
    partition_errors = []
    
    partitions = []
    start_page = 1  
    while start_page <= page_count:
        end_page = min(start_page + PDF_PAGE_PARTITION_SIZE - 1, page_count)
        partitions.append((start_page, end_page))
        start_page = end_page + 1
    
    print(f"Created {len(partitions)} partitions: {partitions}")
    
    for i, (start_page, end_page) in enumerate(partitions):
        print(f"Processing partition {i+1}/{len(partitions)}: pages {start_page}-{end_page}")
        
        try:
            hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_pairs_for_page_range(
                session_pdf_path, start_page, end_page
            )
            print(f"Partition {i+1}: Found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")
            
            if not hyperlink_table_pairs and not unpaired_links:
                print(f"Partition {i+1}: No data extracted, skipping LLM call")
                continue
                
            structured_data_text = format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links)
            
            partition_prompt = create_structured_data_prompt(structured_data_text)
            partition_data, error = call_gemini_api(partition_prompt, expect_json=True)
            
            if error:
                err_msg = f"Partition {i+1} (pages {start_page}-{end_page}): LLM Error: {error}"
                partition_errors.append(err_msg)
                print(err_msg)
                continue
                
            if not isinstance(partition_data, list):
                if isinstance(partition_data, dict) and 'proposal_name' in partition_data:
                    partition_data = [partition_data]
                else:
                    err_msg = f"Partition {i+1} (pages {start_page}-{end_page}): LLM did not return a list. Got: {type(partition_data)}"
                    partition_errors.append(err_msg)
                    print(err_msg)
                    continue
            
            valid_partition_proposals = validate_llm_proposals_response(partition_data)
            if valid_partition_proposals:
                print(f"Partition {i+1}: Successfully extracted {len(valid_partition_proposals)} proposals")
                all_proposals.extend(valid_partition_proposals)
            else:
                print(f"Partition {i+1}: No valid proposals extracted from LLM response {str(partition_data)[:100]}")
        
        except Exception as e:
            err_msg = f"Partition {i+1} (pages {start_page}-{end_page}): General Error: {str(e)}"
            partition_errors.append(err_msg)
            print(f"Error processing partition {i+1}: {e}")
    
    if all_proposals:
        deduplicated_proposals = []
        seen_proposal_identifiers = set()
        for proposal in all_proposals:
            prop_id = (proposal.get('proposal_name'), proposal.get('proposal_link'))
            if prop_id not in seen_proposal_identifiers:
                deduplicated_proposals.append(proposal)
                seen_proposal_identifiers.add(prop_id)
        
        print(f"Successfully extracted {len(deduplicated_proposals)} unique proposals from all partitions")
        if partition_errors: 
             print(f"Encountered {len(partition_errors)} errors during partition processing: {partition_errors}")
        return deduplicated_proposals, None 
    elif partition_errors:
        return None, f"Failed to process long PDF. Errors: {'; '.join(partition_errors)}"
    else: 
        return [], None 

def extract_hyperlink_table_pairs_for_page_range(pdf_path, start_page, end_page):
    """
    Extracts groups of hyperlinks and their single associated table for a specific page range in the PDF.
    Note: start_page and end_page are 1-indexed.
    """
    extracted_pairs = []
    unpaired_hyperlinks = []
    doc_fitz = fitz.open(pdf_path)
    
    start_page_0idx = start_page - 1
    end_page_0idx = end_page - 1
    
    for page_num_0idx in range(start_page_0idx, end_page_0idx + 1):
        if page_num_0idx >= len(doc_fitz): 
            break
            
        page_fitz = doc_fitz[page_num_0idx]
        current_page_1idx = page_num_0idx + 1 
        
        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link['kind'] == fitz.LINK_URI:
                uri = link['uri']
                if ".pdf" in uri.lower(): 
                    continue
                rect = link['from']  
                link_text = page_fitz.get_text("text", clip=rect).strip()
                
                page_hyperlinks.append({
                    'text': link_text if link_text else "N/A",
                    'uri': uri,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1),
                    'page_num_fitz': page_num_0idx 
                })
        
        page_hyperlinks.sort(key=lambda h: h['rect'][1]) 

        page_tables_data = []
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
        except Exception as e:
            tables_on_page_json = []

        for table_json_data in tables_on_page_json:
            table_rows_text = []
            if table_json_data['data']:
                for row_obj in table_json_data['data']:
                    current_row = [cell['text'] for cell in row_obj]
                    table_rows_text.append(current_row)
            
            if not table_rows_text:
                continue
            df_table = pd.DataFrame(table_rows_text) 
            page_tables_data.append({
                'dataframe': df_table,
                'top': table_json_data['top'],
                'left': table_json_data['left'],
                'bottom': table_json_data['top'] + table_json_data['height'],
                'right': table_json_data['left'] + table_json_data['width'],
                'page_num_fitz': page_num_0idx 
            })
            
        page_tables_data.sort(key=lambda t: t['top'])

        hyperlink_cursor = 0 
        
        for table_idx in range(len(page_tables_data)):
            table = page_tables_data[table_idx]
            table_top_y = table['top']
            
            links_for_current_table = []
            
            temp_cursor_for_this_table = hyperlink_cursor
            while temp_cursor_for_this_table < len(page_hyperlinks):
                hyperlink = page_hyperlinks[temp_cursor_for_this_table]
                hyperlink_bottom_y = hyperlink['rect'][3] 

                if hyperlink_bottom_y < table_top_y:
                    links_for_current_table.append({
                        'text': hyperlink['text'],
                        'uri': hyperlink['uri']
                    })
                    temp_cursor_for_this_table += 1
                else:
                    break 
            
            if links_for_current_table:
                extracted_pairs.append({
                    'hyperlinks': links_for_current_table,
                    'table_data': table['dataframe'],
                    'page_num': current_page_1idx 
                })
                hyperlink_cursor = temp_cursor_for_this_table 
        
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            unpaired_hyperlinks.append({
                'hyperlink_text': hyperlink['text'], 
                'uri': hyperlink['uri'],
                'page_num': current_page_1idx 
            })

    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks

def format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links):
    """Format the structured data for the LLM, accommodating grouped hyperlinks."""
    structured_data_text = "STRUCTURED PROPOSAL DATA EXTRACTED FROM PDF:\n\n"
    has_data = False
    
    if hyperlink_table_pairs:
        has_data = True
        structured_data_text += "PROPOSALS WITH VOTING TABLES (a group of proposals may share one table):\n"
        for i, group in enumerate(hyperlink_table_pairs, 1):
            structured_data_text += f"\nGROUP {i} (Page: {group['page_num']}):\n"
            structured_data_text += f"  HYPERLINKS IN THIS GROUP (sharing the table below):\n"
            for link_info in group['hyperlinks']:
                structured_data_text += f"    - TEXT: {link_info['text']}, URI: {link_info['uri']}\n"
            structured_data_text += f"  SHARED VOTING TABLE FOR THIS GROUP:\n"
            table_str = group['table_data'].to_string(index=False, header=True) 
            structured_data_text += f"    {table_str.replace(chr(10), chr(10) + '    ')}\n" 
            structured_data_text += "  " + "-"*50 + "\n"
    
    if unpaired_links:
        has_data = True
        structured_data_text += "\nPROPOSALS WITHOUT INDIVIDUAL VOTING TABLES (may be approved unanimously or in groups):\n"
        for i, link in enumerate(unpaired_links, 1):
            structured_data_text += f"\n{i}. PROPOSAL TEXT: {link['hyperlink_text']}\n" 
            structured_data_text += f"   LINK: {link['uri']}\n"
            structured_data_text += f"   PAGE: {link['page_num']}\n"
    
    if not has_data:
        return "NO DATA EXTRACTED FROM PDF"
        
    return structured_data_text

def create_structured_data_prompt(structured_data_text):
    """Create the LLM prompt for structured data, accommodating grouped hyperlinks."""
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
    Downloads the document if it's a PDF.
    """
    authors_list = []
    document_info = {'link': None, 'type': None, 'local_path': None, 'download_status': 'Not Attempted', 'error': None}
    scrape_status = 'Pending'

    print(f"Fetching proposal details from: {proposal_page_url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(proposal_page_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        html_content = response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {proposal_page_url}: {e}")
        return {'authors_json': None, 'document_info': document_info, 'scrape_status': 'Fetch Failed', 'error': str(e)}

    soup = BeautifulSoup(html_content, 'lxml') 
    base_url = f"{urlparse(proposal_page_url).scheme}://{urlparse(proposal_page_url).netloc}"

    autoria_heading = soup.find(lambda tag: tag.name == "div" and "Autoria" in tag.get_text(strip=True) and "Titulo-Cinzento" in tag.get("class", []))
    if autoria_heading:
        autoria_section_container = autoria_heading.find_parent('div') 
        if autoria_section_container:
            authors_div = autoria_section_container.find_next_sibling('div') 
            if authors_div:
                author_links_tags = authors_div.find_all('a', class_='LinksTram') 
                for link_tag in author_links_tags:
                    name = link_tag.get_text(strip=True)
                    href = link_tag.get('href')
                    if name and href: 
                        authors_list.append({'name': name, 'link': urljoin(base_url, href)})
    
    authors_json = json.dumps(authors_list) if authors_list else None

    doc_search_priority = [
        ('PDF', [lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoPDF')), 
                   lambda s: s.find('a', string=lambda t: t and '[formato PDF]' in t.strip().lower()), 
                   lambda s: next((tag for tag in s.find_all('a', href=True) if '.pdf' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['pdf', 'documento', 'ficheiro', 'texto integral', 'texto final'])), None)]),
        ('DOCX', [lambda s: next((tag for tag in s.find_all('a', href=True) if '.docx' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['docx', 'documento', 'word'])), None)]),
    ]

    found_doc_link_tag = None
    for doc_type, search_methods in doc_search_priority:
        for method in search_methods:
            tag = method(soup)
            if tag and tag.get('href'): 
                found_doc_link_tag = tag
                break 
        if found_doc_link_tag:
            doc_url = urljoin(base_url, found_doc_link_tag.get('href'))
            document_info['link'] = doc_url
            document_info['type'] = doc_type
            
            if doc_type == 'PDF':
                bid_match = re.search(r'BID=(\d+)', proposal_page_url)
                bid_value = bid_match.group(1) if bid_match else hashlib.md5(proposal_page_url.encode()).hexdigest()[:8]
                
                doc_link_text = found_doc_link_tag.get_text(strip=True)
                sane_link_text = re.sub(r'[^\w\s-]', '', doc_link_text).strip()
                sane_link_text = re.sub(r'[-\s]+', '_', sane_link_text)[:50] 

                file_name = f"proposal_{bid_value}_{sane_link_text}.pdf" if sane_link_text else f"proposal_{bid_value}.pdf"
                file_name = re.sub(r'_+', '_', file_name) 

                local_path = os.path.join(download_dir, file_name)
                
                success, msg_or_path = download_file(doc_url, local_path, is_pdf=True)
                if success:
                    document_info['local_path'] = msg_or_path
                    document_info['download_status'] = 'Success'
                else:
                    document_info['download_status'] = 'Download Failed'
                    document_info['error'] = msg_or_path
            else: 
                 document_info['download_status'] = 'Not PDF - Not Downloaded'
            break 
    
    if not document_info['link']:
        document_info['error'] = 'No document link found on page.'
        scrape_status = 'Success (No Doc Link)' 
    else:
        scrape_status = 'Success' 
        
    return {
        'authors_json': authors_json,
        'document_info': document_info,
        'scrape_status': scrape_status,
        'error': document_info['error'] 
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

    last_processed_session_url_in_csv = None
    if not df.empty and 'session_pdf_url' in df.columns and not df['session_pdf_url'].dropna().empty:
        non_na_urls = df['session_pdf_url'].dropna()
        if not non_na_urls.empty:
            last_processed_session_url_in_csv = non_na_urls.iloc[-1]
            print(f"Last session PDF URL recorded in CSV: {last_processed_session_url_in_csv}")

    scraper = ParliamentPDFScraper()
    current_year = datetime.now().year
    _start_year = start_year if start_year else current_year - 5 
    _end_year = end_year if end_year else current_year
    
    print(f"--- Stage 1: Fetching all session PDF links from website for years {_start_year}-{_end_year} ---")
    all_session_pdf_infos_from_web = scraper.scrape_years(start_year=_start_year, end_year=_end_year)
    print(f"Found {len(all_session_pdf_infos_from_web)} potential session PDF links from web.")

    TERMINAL_SUCCESS_STATUSES = {
        'Success', 
        'Completed (No Proposals)', 
        'Completed (No Proposal Doc to Summarize)', 
        'Completed (No Gov Link for Details)'
    }
    
    sessions_to_process_infos = []
    if not df.empty and 'session_pdf_url' in df.columns:
        unique_urls_in_df = df['session_pdf_url'].dropna().unique()
        urls_fully_processed_and_can_skip = set()

        for url_in_df in unique_urls_in_df:
            session_entries = df[df['session_pdf_url'] == url_in_df]
            if session_entries.empty:
                continue

            all_terminal_success = True
            for status in session_entries['overall_status']:
                if pd.isna(status) or status not in TERMINAL_SUCCESS_STATUSES:
                    all_terminal_success = False
                    break
            
            if all_terminal_success:
                urls_fully_processed_and_can_skip.add(url_in_df)
        
        print(f"Identified {len(urls_fully_processed_and_can_skip)} session URLs as fully processed in CSV and potentially skippable.")
        
        sessions_to_process_infos = [
            info for info in all_session_pdf_infos_from_web 
            if info['url'] not in urls_fully_processed_and_can_skip
        ]
    else: 
        sessions_to_process_infos = all_session_pdf_infos_from_web

    if last_processed_session_url_in_csv:
        sessions_to_process_infos.sort(key=lambda x: (x['url'] != last_processed_session_url_in_csv, x.get('date', '1900-01-01'), x['url']))
    else: 
        sessions_to_process_infos.sort(key=lambda x: (x.get('date', '1900-01-01'), x['url']))

    
    print(f"Total sessions to iterate through after filtering: {len(sessions_to_process_infos)}")

    processed_sessions_count = 0
    for session_info in sessions_to_process_infos:
        if max_sessions_to_process and processed_sessions_count >= max_sessions_to_process:
            print(f"Reached max_sessions_to_process limit ({max_sessions_to_process}). Stopping.")
            break
        
        current_session_pdf_url = session_info['url']
        session_year = session_info.get('year') 
        session_date = session_info.get('date') 

        if not session_year:
            try:
                parsed_q = parse_qs(urlparse(current_session_pdf_url).query)
                fich_param = parsed_q.get('Fich', [None])[0]
                if fich_param:
                    match = re.search(r'(\d{4})[-_]\d{2}[-_]\d{2}', fich_param) 
                    if match:
                        session_year = int(match.group(1))
            except: 
                 session_year = _start_year 
        if not session_date: 
            session_date = f"{session_year}-01-01" if session_year else f"{_start_year}-01-01"


        print(f"\n>>> Processing Session PDF URL: {current_session_pdf_url} (Year: {session_year}, Date: {session_date})")

        session_pdf_filename = generate_session_pdf_filename(current_session_pdf_url, session_year)
        session_pdf_local_path_for_download = os.path.join(SESSION_PDF_DIR, session_pdf_filename)
        
        existing_rows_for_session_pdf = df[df['session_pdf_url'] == current_session_pdf_url]
        
        actual_session_pdf_disk_path = None
        session_pdf_download_status_for_df = 'Not Attempted'
        session_pdf_download_error_for_df = None

        if not existing_rows_for_session_pdf.empty:
            summary_rows = existing_rows_for_session_pdf[pd.isna(existing_rows_for_session_pdf['proposal_name_from_session'])]
            ref_row_candidates = summary_rows if not summary_rows.empty else existing_rows_for_session_pdf
            
            for _, ref_row in ref_row_candidates.iterrows():
                # Safe check for download status and path existence
                is_download_success = pd.notna(ref_row['session_pdf_download_status']) and ref_row['session_pdf_download_status'] == 'Success'
                path_exists = pd.notna(ref_row['session_pdf_text_path']) and os.path.exists(ref_row['session_pdf_text_path'])

                if is_download_success and path_exists:
                    print(f"Session PDF already downloaded: {ref_row['session_pdf_text_path']}")
                    actual_session_pdf_disk_path = ref_row['session_pdf_text_path']
                    session_pdf_download_status_for_df = 'Success'
                    break 
            
            if actual_session_pdf_disk_path is None and not ref_row_candidates.empty:
                 if any(pd.notna(status) and status == 'Success' for status in ref_row_candidates['session_pdf_download_status']):
                    print(f"Session PDF {current_session_pdf_url} marked downloaded in CSV but file missing or path invalid. Re-downloading.")
        
        if not actual_session_pdf_disk_path:
            download_success, msg_or_path = download_file(current_session_pdf_url, session_pdf_local_path_for_download)
            if download_success:
                actual_session_pdf_disk_path = msg_or_path
                session_pdf_download_status_for_df = 'Success'
            else:
                session_pdf_download_status_for_df = 'Download Failed'
                session_pdf_download_error_for_df = str(msg_or_path)
                
                placeholder_indices = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                         (df['proposal_name_from_session'].isna())].index
                
                if placeholder_indices.empty:
                    new_idx = len(df)
                    df.loc[new_idx, 'session_pdf_url'] = current_session_pdf_url
                    df.loc[new_idx, 'session_year'] = session_year
                    df.loc[new_idx, 'session_date'] = session_date
                    for col in get_dataframe_columns():
                        if col not in ['session_pdf_url', 'session_year', 'session_date']:
                            df.loc[new_idx, col] = pd.NA 
                else:
                    new_idx = placeholder_indices[0] 

                df.loc[new_idx, 'session_pdf_download_status'] = session_pdf_download_status_for_df
                df.loc[new_idx, 'last_error_message'] = session_pdf_download_error_for_df
                df.loc[new_idx, 'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                df.loc[new_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
                
                other_indices = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                   (df['proposal_name_from_session'].notna())].index
                for idx_other in other_indices:
                    df.loc[idx_other, 'session_pdf_download_status'] = session_pdf_download_status_for_df
                    df.loc[idx_other, 'last_error_message'] = session_pdf_download_error_for_df
                    df.loc[idx_other, 'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                    df.loc[idx_other, 'last_processed_timestamp'] = datetime.now().isoformat()

                save_dataframe(df)
                processed_sessions_count += 1 
                continue

        proposals_from_llm = None
        session_parse_status_for_df = 'Not Attempted'
        session_parse_error_for_df = None
        run_stage2_llm_parse = True

        if not existing_rows_for_session_pdf.empty:
            summary_row_no_proposals_status = existing_rows_for_session_pdf[
                (pd.notna(existing_rows_for_session_pdf['session_parse_status'])) &
                (existing_rows_for_session_pdf['session_parse_status'] == 'LLM Parsed - No Propostas Encontradas') &
                (pd.isna(existing_rows_for_session_pdf['proposal_name_from_session']))
            ]
            
            proposal_rows = existing_rows_for_session_pdf[pd.notna(existing_rows_for_session_pdf['proposal_name_from_session'])]
            all_proposal_rows_parsed_successfully = True
            if not proposal_rows.empty:
                all_proposal_rows_parsed_successfully = all(
                    pd.notna(status) and status == 'Success' for status in proposal_rows['session_parse_status'].dropna() # Ensure notna before compare
                )
            else: 
                all_proposal_rows_parsed_successfully = True 

            # Check if any row has session_parse_status 'Success' (handles case of 0 proposals found by LLM but not explicitly marked)
            any_row_parsed_successfully = any(
                pd.notna(status) and status == 'Success' for status in existing_rows_for_session_pdf['session_parse_status']
            )

            if not summary_row_no_proposals_status.empty or \
               (not proposal_rows.empty and all_proposal_rows_parsed_successfully) or \
               (proposal_rows.empty and any_row_parsed_successfully):

                print(f"Session PDF {current_session_pdf_url} appears to be parsed previously. Reconstructing proposals from CSV if any.")
                run_stage2_llm_parse = False
                proposals_from_llm = []
                for _, row in existing_rows_for_session_pdf.iterrows():
                    if pd.notna(row['proposal_name_from_session']): 
                        try:
                            voting_summary_obj = json.loads(row['voting_details_json']) if pd.notna(row['voting_details_json']) else None
                        except json.JSONDecodeError:
                            voting_summary_obj = None 
                        proposals_from_llm.append({
                            'proposal_name': row['proposal_name_from_session'],
                            'proposal_link': row['proposal_gov_link'],
                            'voting_summary': voting_summary_obj,
                            'proposal_approval_status': row['proposal_approval_status'] # Corrected key
                        })
                if not proposals_from_llm and not summary_row_no_proposals_status.empty:
                    session_parse_status_for_df = 'LLM Parsed - No Propostas Encontradas'
                elif proposals_from_llm : 
                    session_parse_status_for_df = 'Success'
                elif existing_rows_for_session_pdf['session_parse_status'].notna().any(): 
                    session_parse_status_for_df = existing_rows_for_session_pdf['session_parse_status'].dropna().iloc[0] \
                        if not existing_rows_for_session_pdf['session_parse_status'].dropna().empty else 'Unknown (Reconstructed)'
                else:
                     session_parse_status_for_df = 'Unknown (Reconstructed)'


        if run_stage2_llm_parse:
            print(f"Running LLM parse for session PDF: {actual_session_pdf_disk_path}")
            indices_to_drop = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                 (df['proposal_name_from_session'].notna())].index
            if not indices_to_drop.empty:
                print(f"Dropping {len(indices_to_drop)} old proposal entries for this session before re-parsing.")
                df.drop(indices_to_drop, inplace=True)
                df.reset_index(drop=True, inplace=True) 

            proposals_from_llm, llm_error = extract_votes_from_session_pdf_text(actual_session_pdf_disk_path)
            
            if llm_error:
                session_parse_status_for_df = f'LLM Parse Failed: {llm_error}'
                session_parse_error_for_df = llm_error
            elif not proposals_from_llm: 
                session_parse_status_for_df = 'LLM Parsed - No Propostas Encontradas'
            else: 
                session_parse_status_for_df = 'Success'
        
        if session_parse_error_for_df or (session_parse_status_for_df == 'LLM Parsed - No Propostas Encontradas' and not proposals_from_llm) :
            summary_row_indices = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                     (df['proposal_name_from_session'].isna())].index
            
            summary_idx_to_update = -1
            if not summary_row_indices.empty:
                summary_idx_to_update = summary_row_indices[0]
            else: 
                summary_idx_to_update = len(df)
                df.loc[summary_idx_to_update, 'session_pdf_url'] = current_session_pdf_url
                for col in get_dataframe_columns():
                    if col not in ['session_pdf_url']: df.loc[summary_idx_to_update, col] = pd.NA


            df.loc[summary_idx_to_update, 'session_year'] = session_year
            df.loc[summary_idx_to_update, 'session_date'] = session_date
            df.loc[summary_idx_to_update, 'session_pdf_text_path'] = actual_session_pdf_disk_path
            df.loc[summary_idx_to_update, 'session_pdf_download_status'] = 'Success' 
            df.loc[summary_idx_to_update, 'session_parse_status'] = session_parse_status_for_df
            df.loc[summary_idx_to_update, 'last_error_message'] = session_parse_error_for_df 
            df.loc[summary_idx_to_update, 'overall_status'] = 'Failed Stage 2 (LLM Session Parse)' if session_parse_error_for_df else 'Completed (No Proposals)'
            df.loc[summary_idx_to_update, 'last_processed_timestamp'] = datetime.now().isoformat()
            
            if run_stage2_llm_parse: 
                 indices_to_drop = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                      (df['proposal_name_from_session'].notna())].index
                 if not indices_to_drop.empty:
                     df.drop(indices_to_drop, inplace=True)
                     df.reset_index(drop=True, inplace=True)

            save_dataframe(df)
            processed_sessions_count += 1
            continue

        if proposals_from_llm is None or (not proposals_from_llm and not run_stage2_llm_parse): 
            summary_row_indices = df[(df['session_pdf_url'] == current_session_pdf_url) & 
                                     (df['proposal_name_from_session'].isna())].index
            if not summary_row_indices.empty:
                summary_idx = summary_row_indices[0]
                current_overall_status_val = df.loc[summary_idx, 'overall_status']
                is_terminal = pd.notna(current_overall_status_val) and current_overall_status_val in TERMINAL_SUCCESS_STATUSES
                if pd.isna(current_overall_status_val) or not is_terminal:
                    df.loc[summary_idx, 'overall_status'] = 'Completed (No Proposals)' 
                    df.loc[summary_idx, 'session_parse_status'] = session_parse_status_for_df 
                    df.loc[summary_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else: 
                summary_idx = len(df)
                df.loc[summary_idx, 'session_pdf_url'] = current_session_pdf_url
                df.loc[summary_idx, 'session_year'] = session_year
                df.loc[summary_idx, 'session_date'] = session_date
                df.loc[summary_idx, 'session_pdf_text_path'] = actual_session_pdf_disk_path
                df.loc[summary_idx, 'session_pdf_download_status'] = 'Success'
                df.loc[summary_idx, 'session_parse_status'] = session_parse_status_for_df
                df.loc[summary_idx, 'overall_status'] = 'Completed (No Proposals)'
                df.loc[summary_idx, 'last_processed_timestamp'] = datetime.now().isoformat()

            print(f"No proposals found or reconstructed for {current_session_pdf_url}.")
            save_dataframe(df)
            processed_sessions_count += 1
            continue
            
        print(f"Found/Reconstructed {len(proposals_from_llm)} proposals for {current_session_pdf_url}.")

        for proposal_data_from_llm in proposals_from_llm:
            proposal_name = proposal_data_from_llm.get('proposal_name')
            proposal_gov_link = proposal_data_from_llm.get('proposal_link')
            voting_summary = proposal_data_from_llm.get('voting_summary')
            approval_status_from_llm = proposal_data_from_llm.get('proposal_approval_status') # Corrected key

            if not proposal_name: 
                print(f"Skipping proposal with no name from LLM for session {current_session_pdf_url}")
                continue

            proposal_row_match_indices = df[
                (df['session_pdf_url'] == current_session_pdf_url) &
                (df['proposal_name_from_session'] == proposal_name) &
                ( (df['proposal_gov_link'] == proposal_gov_link) if pd.notna(proposal_gov_link) else df['proposal_gov_link'].isna() )
            ].index
            
            row_idx = -1
            if proposal_row_match_indices.empty:
                row_idx = len(df) 
                df.loc[row_idx, 'session_pdf_url'] = current_session_pdf_url
                df.loc[row_idx, 'session_year'] = session_year 
                df.loc[row_idx, 'proposal_name_from_session'] = proposal_name
                for col in get_dataframe_columns():
                    if col not in ['session_pdf_url', 'session_year', 'proposal_name_from_session']:
                         df.loc[row_idx, col] = pd.NA 
            else:
                row_idx = proposal_row_match_indices[0] 

            df.loc[row_idx, 'session_date'] = session_date 
            df.loc[row_idx, 'session_pdf_text_path'] = actual_session_pdf_disk_path
            df.loc[row_idx, 'session_pdf_download_status'] = 'Success' 
            df.loc[row_idx, 'proposal_gov_link'] = proposal_gov_link
            df.loc[row_idx, 'voting_details_json'] = json.dumps(voting_summary) if voting_summary else None
            df.loc[row_idx, 'session_parse_status'] = session_parse_status_for_df 
            df.loc[row_idx, 'proposal_approval_status'] = approval_status_from_llm
            
            current_overall_status = df.loc[row_idx, 'overall_status']
            is_current_overall_status_terminal = pd.notna(current_overall_status) and current_overall_status in TERMINAL_SUCCESS_STATUSES
            
            is_last_processed_and_not_terminal = (current_session_pdf_url == last_processed_session_url_in_csv and \
                                                  (pd.isna(current_overall_status) or not is_current_overall_status_terminal))

            if pd.isna(current_overall_status) or not is_current_overall_status_terminal or is_last_processed_and_not_terminal:
                 df.loc[row_idx, 'overall_status'] = 'Pending Further Stages'
                 df.loc[row_idx, 'last_error_message'] = pd.NA # Clear previous errors
                 df.loc[row_idx, 'proposal_details_scrape_status'] = pd.NA
                 df.loc[row_idx, 'proposal_doc_download_status'] = pd.NA
                 df.loc[row_idx, 'proposal_summarize_status'] = pd.NA


            # --- Stage 3: Get Proposal Details & Document ---
            needs_stage3_run = False
            if pd.notna(proposal_gov_link) and isinstance(proposal_gov_link, str) and proposal_gov_link.startswith("http"):
                current_scrape_status = df.loc[row_idx, 'proposal_details_scrape_status']
                scrape_status_is_na = pd.isna(current_scrape_status)

                is_terminal_status_for_stage3 = False
                if not scrape_status_is_na:
                    is_terminal_status_for_stage3 = current_scrape_status in ['Success', 'Success (No Doc Link)', 'No Gov Link', 'Fetch Failed']

                rerun_last_session_for_stage3 = False
                if current_session_pdf_url == last_processed_session_url_in_csv:
                    is_perfect_stage3_success = False
                    if not scrape_status_is_na and current_scrape_status in ['Success', 'Success (No Doc Link)']:
                        is_perfect_stage3_success = True
                    if not is_perfect_stage3_success:
                        rerun_last_session_for_stage3 = True
                
                if scrape_status_is_na or not is_terminal_status_for_stage3 or rerun_last_session_for_stage3:
                    needs_stage3_run = True
            else: 
                current_overall_status_for_else = df.loc[row_idx, 'overall_status']
                update_overall_status_to_no_gov_link = False
                if pd.notna(current_overall_status_for_else):
                    if current_overall_status_for_else == 'Pending Further Stages':
                        update_overall_status_to_no_gov_link = True
                elif pd.isna(current_overall_status_for_else): 
                    update_overall_status_to_no_gov_link = True
                
                if update_overall_status_to_no_gov_link:
                    df.loc[row_idx, 'overall_status'] = 'Completed (No Gov Link for Details)'
                df.loc[row_idx, 'proposal_details_scrape_status'] = 'No Gov Link'


            if needs_stage3_run:
                print(f"  Fetching details for proposal: {proposal_name} from {proposal_gov_link}")
                details_result = fetch_proposal_details_and_download_doc(proposal_gov_link, PROPOSAL_DOC_DIR)
                df.loc[row_idx, 'proposal_authors_json'] = details_result['authors_json']
                df.loc[row_idx, 'proposal_document_url'] = details_result['document_info']['link']
                df.loc[row_idx, 'proposal_document_type'] = details_result['document_info']['type']
                df.loc[row_idx, 'proposal_document_local_path'] = details_result['document_info']['local_path']
                df.loc[row_idx, 'proposal_doc_download_status'] = details_result['document_info']['download_status']
                df.loc[row_idx, 'proposal_details_scrape_status'] = details_result['scrape_status']
                
                if details_result['error'] and \
                   (pd.isna(details_result['scrape_status']) or details_result['scrape_status'] != 'Success (No Doc Link)'): 
                    df.loc[row_idx, 'last_error_message'] = str(details_result['error'])
                    df.loc[row_idx, 'overall_status'] = 'Failed Stage 3 (Proposal Details Scrape)'
                elif pd.notna(df.loc[row_idx, 'overall_status']) and df.loc[row_idx, 'overall_status'] == 'Pending Further Stages': 
                    df.loc[row_idx, 'overall_status'] = 'Pending Stage 4' 

            # --- Stage 4: Summarize Proposal Document ---
            needs_stage4_run = False
            doc_dl_status_s4 = df.loc[row_idx, 'proposal_doc_download_status']
            doc_is_successful_s4 = pd.notna(doc_dl_status_s4) and doc_dl_status_s4 == 'Success'
            
            overall_status_s4_val = df.loc[row_idx, 'overall_status']
            overall_status_s4_str = str(overall_status_s4_val) # Safe for startswith

            if doc_is_successful_s4 and \
               pd.notna(df.loc[row_idx, 'proposal_document_local_path']) and \
               not overall_status_s4_str.startswith('Failed Stage 3'):
                
                current_summary_status_s4 = df.loc[row_idx, 'proposal_summarize_status']
                # This original condition structure is safe due to leading pd.isna()
                if pd.isna(current_summary_status_s4) or \
                   (pd.notna(current_summary_status_s4) and current_summary_status_s4 != 'Success') or \
                   (current_session_pdf_url == last_processed_session_url_in_csv and \
                    (pd.isna(current_summary_status_s4) or (pd.notna(current_summary_status_s4) and current_summary_status_s4 != 'Success'))):
                    needs_stage4_run = True
            
            if needs_stage4_run:
                proposal_doc_disk_path_for_summary = df.loc[row_idx, 'proposal_document_local_path']
                print(f"  Summarizing proposal document: {proposal_doc_disk_path_for_summary}")
                summary_data, summary_err = summarize_proposal_text(proposal_doc_disk_path_for_summary)
                if summary_err:
                    df.loc[row_idx, 'proposal_summarize_status'] = f'LLM Summary Failed: {summary_err}'
                    df.loc[row_idx, 'last_error_message'] = summary_err
                    df.loc[row_idx, 'overall_status'] = 'Failed Stage 4 (LLM Summary)'
                else:
                    df.loc[row_idx, 'proposal_summary_general'] = summary_data['general_summary']
                    df.loc[row_idx, 'proposal_summary_analysis'] = summary_data['critical_analysis']
                    df.loc[row_idx, 'proposal_summary_fiscal_impact'] = summary_data['fiscal_impact']
                    df.loc[row_idx, 'proposal_summary_colloquial'] = summary_data['colloquial_summary']
                    df.loc[row_idx, 'proposal_category'] = summary_data['categories'] 
                    df.loc[row_idx, 'proposal_short_title'] = summary_data['short_title'] 
                    df.loc[row_idx, 'proposal_proposing_party'] = summary_data['proposing_party']
                    df.loc[row_idx, 'proposal_summarize_status'] = 'Success'
                    df.loc[row_idx, 'overall_status'] = 'Success' 
            
            current_os_final = df.loc[row_idx, 'overall_status']
            is_pending_for_final_update = False
            if pd.notna(current_os_final):
                if current_os_final in ['Pending Further Stages', 'Pending Stage 4']:
                    is_pending_for_final_update = True
            elif pd.isna(current_os_final):
                 is_pending_for_final_update = True

            if is_pending_for_final_update:
                summarize_status_val = df.loc[row_idx, 'proposal_summarize_status']
                is_summarize_success = pd.notna(summarize_status_val) and summarize_status_val == 'Success'
                
                doc_dl_status_final = df.loc[row_idx, 'proposal_doc_download_status']
                details_scrape_status_final = df.loc[row_idx, 'proposal_details_scrape_status']

                if is_summarize_success:
                    df.loc[row_idx, 'overall_status'] = 'Success'
                else: 
                    doc_not_success_final = True 
                    if pd.notna(doc_dl_status_final) and doc_dl_status_final == 'Success':
                        doc_not_success_final = False 
                    
                    details_scrape_is_success_variant_final = False
                    if pd.notna(details_scrape_status_final) and details_scrape_status_final in ['Success', 'Success (No Doc Link)']:
                        details_scrape_is_success_variant_final = True
                    
                    details_scrape_is_no_gov_link_final = False
                    if pd.notna(details_scrape_status_final) and details_scrape_status_final == 'No Gov Link':
                        details_scrape_is_no_gov_link_final = True

                    if doc_not_success_final and details_scrape_is_success_variant_final:
                        df.loc[row_idx, 'overall_status'] = 'Completed (No Proposal Doc to Summarize)'
                    elif details_scrape_is_no_gov_link_final:
                         df.loc[row_idx, 'overall_status'] = 'Completed (No Gov Link for Details)'

            df.loc[row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            save_dataframe(df) 
        
        processed_sessions_count += 1

    print("\n--- Pipeline Run Finished ---")
    if not df.empty:
        print("Overall Status Counts:")
        print(df['overall_status'].value_counts(dropna=False))
    else:
        print("DataFrame is empty.")

if __name__ == "__main__":
    # Example: run_pipeline(start_year=2022, end_year=2023, max_sessions_to_process=10)
    # run_pipeline() # Process last 5 years, all sessions
    run_pipeline(start_year=2020, end_year=2020, max_sessions_to_process=None)
