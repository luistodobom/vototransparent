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
import concurrent.futures  # Add this import for parallel processing
import threading # Add this import for Lock

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
MAX_WORKERS = 4  # Number of parallel threads to use for proposal processing

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
            df[col] = None # Or pd.NA
    df = df[expected_columns] # Reorder columns to expected order
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
            if date_elem and time_elem:
                try:
                    day_month = date_elem.get_text(strip=True)  # e.g., "19.12"
                    year_text = time_elem.get_text(strip=True)  # e.g., "2024"
                    
                    if '.' in day_month and year_text.isdigit():
                        day, month = day_month.split('.')
                        # Convert to ISO date format (YYYY-MM-DD)
                        session_date = f"{year_text}-{month.zfill(2)}-{day.zfill(2)}"
                except (ValueError, AttributeError) as e:
                    print(f"Error parsing date from {day_month} and {year_text}: {e}")
                    session_date = None
            
            # Find PDF links within this calendar detail
            all_anchor_tags = calendar_detail.find_all('a', href=True)
            
            for link_tag in all_anchor_tags:
                href = link_tag.get('href', '')
                text_content = link_tag.get_text(strip=True)

                # Skip supplementary guides
                if "guião suplementar" in text_content.lower():
                    print(f"Skipping supplementary guide: {text_content} ({href})")
                    continue
                
                # Prioritize links that look like direct PDF links related to voting summaries
                # Example: DARxxx.pdf, _Votacoes_, _ResultadosVotacao_
                if (href.lower().endswith('.pdf') and 
                    any(kw in href.lower() for kw in ['votacoe', 'resultado', 'dar', 'serieii'])): # Added serieii based on typical DAR naming
                    full_url = urljoin("https://www.parlamento.pt", href)
                    # Further check if text implies it's a voting summary
                    if "votaç" in text_content.lower() or "diário" in text_content.lower() or "reunião plenária" in text_content.lower():
                        pdf_links.append({
                            'url': full_url, 
                            'year': year, 
                            'date': session_date,
                            'text': text_content, 
                            'type': 'direct_pdf_votacao'
                        })
                # Parameterized links that often lead to PDFs
                elif ('doc.pdf' in href.lower() or 'path=' in href.lower() or 'downloadfile' in href.lower()):
                     if "votaç" in text_content.lower() or "diário" in text_content.lower():
                        full_url = urljoin("https://www.parlamento.pt", href)
                        pdf_links.append({
                            'url': full_url, 
                            'year': year, 
                            'date': session_date,
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
        for year in range(start_year, end_year + 1):
            html_content = self.get_page_content(year)
            if html_content:
                year_links = self.extract_pdf_links_from_html(html_content, year)
                all_pdf_links.extend(year_links)
            time.sleep(1) # Be respectful to the server
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
                rect = link['from']  # fitz.Rect object for the link area
                link_text = page_fitz.get_text("text", clip=rect).strip()
                
                page_hyperlinks.append({
                    'text': link_text if link_text else "N/A",
                    'uri': uri,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1),
                    'page_num_fitz': page_num # 0-indexed for internal use
                })
        
        # Sort hyperlinks by their vertical position (top y-coordinate: rect[1])
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
            # print(f"Warning: Could not extract tables from page {page_num + 1} with tabula: {e}")
            tables_on_page_json = []

        for table_json_data in tables_on_page_json:
            table_rows_text = []
            if table_json_data['data']: # Check if data is not empty
                for row_obj in table_json_data['data']:
                    current_row = [cell['text'] for cell in row_obj]
                    table_rows_text.append(current_row)
            
            if not table_rows_text: # Skip if table has no text data
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
            
        # Sort tables by their vertical position (top y-coordinate)
        page_tables_data.sort(key=lambda t: t['top'])

        # 3. Correlate hyperlinks and tables on the page
        hyperlink_cursor = 0 # Index of the next hyperlink to consider assigning
        
        for table_idx in range(len(page_tables_data)):
            table = page_tables_data[table_idx]
            table_top_y = table['top']
            
            links_for_current_table = []
            
            # Iterate through available hyperlinks to see if they are above the current table
            temp_cursor_for_this_table = hyperlink_cursor
            while temp_cursor_for_this_table < len(page_hyperlinks):
                hyperlink = page_hyperlinks[temp_cursor_for_this_table]
                hyperlink_bottom_y = hyperlink['rect'][3]

                if hyperlink_bottom_y < table_top_y:
                    # This hyperlink is above the current table and not yet assigned
                    links_for_current_table.append({
                        'text': hyperlink['text'],
                        'uri': hyperlink['uri']
                        # Optionally, include rect or other details:
                        # 'rect': hyperlink['rect'], 
                        # 'page_num_fitz': hyperlink['page_num_fitz']
                    })
                    temp_cursor_for_this_table += 1
                else:
                    # This hyperlink is below or at the same level as the table's top,
                    # so it (and subsequent hyperlinks) belong to later tables or are unpaired.
                    break 
            
            if links_for_current_table:
                extracted_pairs.append({
                    'hyperlinks': links_for_current_table, # Now a list of hyperlink dicts
                    'table_data': table['dataframe'],
                    'page_num': table['page_num_fitz'] + 1 # User-friendly page number
                })
                # Advance the main hyperlink_cursor past the links assigned to this table
                hyperlink_cursor = temp_cursor_for_this_table 
        
        # Add any remaining hyperlinks (those after all tables on the page, or if no tables) to unpaired_hyperlinks
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            unpaired_hyperlinks.append({
                'hyperlink_text': hyperlink['text'], # Keep 'hyperlink_text' for consistency with old unpaired structure
                'uri': hyperlink['uri'],
                'page_num': hyperlink['page_num_fitz'] + 1
            })

    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks


def extract_votes_from_session_pdf_text(session_pdf_path):
    """Enhanced voting extraction using manual PDF parsing followed by LLM processing."""
    print(f"Starting enhanced PDF parsing for: {session_pdf_path}")
    
    # Check if PDF needs to be partitioned
    try:
        doc_fitz = fitz.open(session_pdf_path)
        page_count = len(doc_fitz)
        doc_fitz.close()
        print(f"PDF has {page_count} pages")
        
        # If PDF is longer than partition size, process in chunks
        if page_count > PDF_PAGE_PARTITION_SIZE:
            return process_long_pdf_in_chunks(session_pdf_path, page_count)
    except Exception as e:
        print(f"Error checking PDF page count: {e}")
    
    # For smaller PDFs, continue with regular processing
    try:
        hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_pairs_and_unpaired_links(session_pdf_path)
        print(f"Manual parsing found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")
    except Exception as e:
        print(f"Manual PDF parsing failed: {e}. Falling back to original text extraction method.")
        # Fallback to original method
        text, extract_error = extract_text_from_pdf(session_pdf_path)
        if extract_error:
            return None, f"PDF text extraction failed: {extract_error}"
    
    # Format the structured data for the LLM
    structured_data_text = format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links)
    
    # Call LLM with the structured data
    extracted_data, error = call_gemini_api(create_structured_data_prompt(structured_data_text), expect_json=True)
    if error:
        return None, f"LLM API call failed: {error}"
    if not isinstance(extracted_data, list):
        return None, f"LLM did not return a list as expected. Got: {type(extracted_data)}"
    
    # Validate results
    valid_proposals = validate_llm_proposals_response(extracted_data)
    
    if not valid_proposals and extracted_data:
         return None, f"LLM returned data but no valid proposal structures found. Raw: {str(extracted_data)[:500]}"
    elif not valid_proposals and not extracted_data:
        return None, "LLM returned no processable proposal data."

    print(f"Successfully extracted {len(valid_proposals)} proposals using enhanced parsing method")
    return valid_proposals, None

def process_long_pdf_in_chunks(session_pdf_path, page_count):
    """Process a long PDF by partitioning it into smaller chunks."""
    print(f"Processing long PDF ({page_count} pages) in chunks of {PDF_PAGE_PARTITION_SIZE} pages")
    
    all_proposals = []
    partition_errors = []
    
    # Create page range partitions
    partitions = []
    start_page = 1  # 1-indexed for tabula
    while start_page <= page_count:
        end_page = min(start_page + PDF_PAGE_PARTITION_SIZE - 1, page_count)
        partitions.append((start_page, end_page))
        start_page = end_page + 1
    
    print(f"Created {len(partitions)} partitions: {partitions}")
    
    # Process each partition
    for i, (start_page, end_page) in enumerate(partitions):
        print(f"Processing partition {i+1}/{len(partitions)}: pages {start_page}-{end_page}")
        
        # Extract hyperlinks and tables for this partition only
        try:
            hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_pairs_for_page_range(
                session_pdf_path, start_page, end_page
            )
            print(f"Partition {i+1}: Found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")
            
            if not hyperlink_table_pairs and not unpaired_links:
                print(f"Partition {i+1}: No data extracted, skipping LLM call")
                continue
                
            # Format structured data for this partition
            structured_data_text = format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links)
            
            # Call LLM with the structured data for this partition
            partition_prompt = create_structured_data_prompt(structured_data_text)
            partition_data, error = call_gemini_api(partition_prompt, expect_json=True)
            
            if error:
                partition_errors.append(f"Partition {i+1} (pages {start_page}-{end_page}): {error}")
                print(f"Error processing partition {i+1}: {error}")
                continue
                
            if not isinstance(partition_data, list):
                partition_errors.append(f"Partition {i+1} (pages {start_page}-{end_page}): LLM did not return a list")
                print(f"Partition {i+1}: LLM did not return a list as expected. Got: {type(partition_data)}")
                continue
            
            # Validate and add valid proposals from this partition
            valid_partition_proposals = validate_llm_proposals_response(partition_data)
            if valid_partition_proposals:
                print(f"Partition {i+1}: Successfully extracted {len(valid_partition_proposals)} proposals")
                all_proposals.extend(valid_partition_proposals)
            else:
                print(f"Partition {i+1}: No valid proposals extracted")
        
        except Exception as e:
            partition_errors.append(f"Partition {i+1} (pages {start_page}-{end_page}): {str(e)}")
            print(f"Error processing partition {i+1}: {e}")
    
    # Check results and return
    if all_proposals:
        # Deduplicate proposals based on name
        deduplicated_proposals = []
        seen_proposal_names = set()
        for proposal in all_proposals:
            if proposal.get('proposal_name') not in seen_proposal_names:
                deduplicated_proposals.append(proposal)
                seen_proposal_names.add(proposal.get('proposal_name'))
        
        print(f"Successfully extracted {len(deduplicated_proposals)} unique proposals from all partitions")
        return deduplicated_proposals, None
    elif partition_errors:
        # Return the first error message or a combined error message
        return None, f"Failed to process long PDF: {partition_errors[0]}"
    else:
        return None, "No proposals found in any partition of the long PDF"

def extract_hyperlink_table_pairs_for_page_range(pdf_path, start_page, end_page):
    """
    Extracts groups of hyperlinks and their single associated table for a specific page range in the PDF.
    Also returns a list of hyperlinks within that page range that did not have a table immediately following them.
    A table is associated with all hyperlinks that appear directly before it
    on the same page and after any previously processed table or its associated hyperlinks.
    Note: start_page and end_page are 1-indexed.
    """
    extracted_pairs = []
    unpaired_hyperlinks = []
    doc_fitz = fitz.open(pdf_path)
    
    # Convert to 0-indexed for PyMuPDF
    start_page_0idx = start_page - 1
    end_page_0idx = end_page - 1
    
    # Process only the specified page range
    for page_num in range(start_page_0idx, end_page_0idx + 1):
        if page_num >= len(doc_fitz):
            break
            
        page_fitz = doc_fitz[page_num]
        
        # 1. Extract hyperlinks from the current page
        page_hyperlinks = []
        links = page_fitz.get_links()
        for link in links:
            if link['kind'] == fitz.LINK_URI:
                uri = link['uri']
                if ".pdf" in uri.lower():
                    continue
                rect = link['from']  # fitz.Rect object for the link area
                link_text = page_fitz.get_text("text", clip=rect).strip()
                
                page_hyperlinks.append({
                    'text': link_text if link_text else "N/A",
                    'uri': uri,
                    'rect': (rect.x0, rect.y0, rect.x1, rect.y1),
                    'page_num_fitz': page_num # 0-indexed for internal use
                })
        
        page_hyperlinks.sort(key=lambda h: h['rect'][1])

        # 2. Extract tables from the current page using tabula
        page_tables_data = []
        try:
            # Tabula pages are 1-indexed
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
                'page_num_fitz': page_num # Keep 0-indexed for internal consistency
            })
            
        page_tables_data.sort(key=lambda t: t['top'])

        # 3. Correlate hyperlinks and tables on the page (NEW LOGIC APPLIED HERE)
        hyperlink_cursor = 0 # Index of the next hyperlink to consider assigning
        
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
                    'page_num': table['page_num_fitz'] + 1 # User-friendly 1-indexed page number
                })
                hyperlink_cursor = temp_cursor_for_this_table 
        
        # Add any remaining hyperlinks on this page to unpaired_hyperlinks
        for i in range(hyperlink_cursor, len(page_hyperlinks)):
            hyperlink = page_hyperlinks[i]
            unpaired_hyperlinks.append({
                'hyperlink_text': hyperlink['text'], # Keep 'hyperlink_text' for consistency
                'uri': hyperlink['uri'],
                'page_num': hyperlink['page_num_fitz'] + 1 # User-friendly 1-indexed page number
            })

    doc_fitz.close()
    return extracted_pairs, unpaired_hyperlinks

def format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links):
    """Format the structured data for the LLM, accommodating grouped hyperlinks."""
    structured_data_text = "STRUCTURED PROPOSAL DATA EXTRACTED FROM PDF:\n\n"
    
    # Add hyperlink-table pairs (groups of hyperlinks sharing one table)
    if hyperlink_table_pairs:
        structured_data_text += "PROPOSALS WITH VOTING TABLES (a group of proposals may share one table):\n"
        for i, group in enumerate(hyperlink_table_pairs, 1):
            structured_data_text += f"\nGROUP {i} (Page: {group['page_num']}):\n"
            structured_data_text += f"  HYPERLINKS IN THIS GROUP (sharing the table below):\n"
            for link_info in group['hyperlinks']:
                structured_data_text += f"    - TEXT: {link_info['text']}, URI: {link_info['uri']}\n"
            structured_data_text += f"  SHARED VOTING TABLE FOR THIS GROUP:\n"
            # Convert DataFrame to string representation
            table_str = group['table_data'].to_string(index=False, header=True) # Added header for clarity
            structured_data_text += f"    {table_str.replace(chr(10), chr(10) + '    ')}\n" # Indent table lines
            structured_data_text += "  " + "-"*50 + "\n"
    
    # Add unpaired links
    if unpaired_links:
        structured_data_text += "\nPROPOSALS WITHOUT INDIVIDUAL VOTING TABLES (may be approved unanimously or in groups):\n"
        for i, link in enumerate(unpaired_links, 1):
            structured_data_text += f"\n{i}. PROPOSAL TEXT: {link['hyperlink_text']}\n" # Changed to 'PROPOSAL TEXT'
            structured_data_text += f"   LINK: {link['uri']}\n"
            structured_data_text += f"   PAGE: {link['page_num']}\n"
    
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
    for item in extracted_data:
        if isinstance(item, dict) and 'proposal_name' in item:
            valid_proposals.append(item)
        else:
            print(f"Warning: LLM returned an invalid item structure: {item}")
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

    # Extract Author Information
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

    # Extract Document Link (prioritizing PDF)
    doc_search_priority = [
        ('PDF', [lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoPDF')),
                   lambda s: s.find('a', string=lambda t: t and '[formato PDF]' in t.strip()),
                   lambda s: next((tag for tag in s.find_all('a', href=True) if '.pdf' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['pdf', 'documento', 'ficheiro', 'texto integral'])), None)]),
        ('DOCX', [lambda s: next((tag for tag in s.find_all('a', href=True) if '.docx' in tag.get('href','').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['docx', 'documento'])), None)]),
        # Add other types if needed
    ]

    for doc_type, search_methods in doc_search_priority:
        link_tag = None
        for method in search_methods:
            tag = method(soup)
            if tag and tag.get('href'):
                link_tag = tag; break
        if link_tag:
            doc_url = urljoin(base_url, link_tag.get('href'))
            document_info['link'] = doc_url
            document_info['type'] = doc_type
            
            # Try to download if it's a PDF
            if doc_type == 'PDF':
                bid_value = parse_qs(urlparse(proposal_page_url).query).get('BID', [None])[0]
                file_name = f"proposal_{bid_value if bid_value else proposal_page_url.split('=')[-1]}.pdf"
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
        'error': document_info['error'] # Propagate download error if any
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
    
    # Validate the returned JSON structure
    if not isinstance(summary_data, dict):
        return None, f"LLM did not return a JSON object as expected. Got: {type(summary_data)}"
    
    required_fields = ['general_summary', 'critical_analysis', 'fiscal_impact', 'colloquial_summary', 'categories', 'short_title', 'proposing_party']
    for field in required_fields:
        if field not in summary_data:
            return None, f"Missing required field '{field}' in LLM response: {summary_data}"
    
    # Validate that categories is a list
    if not isinstance(summary_data['categories'], list):
        return None, f"Field 'categories' should be a list, got: {type(summary_data['categories'])}"
    
    # Convert categories list to JSON string for storage
    summary_data['categories'] = json.dumps(summary_data['categories'])
    
    return summary_data, None

def generate_session_pdf_filename(session_pdf_url, session_year):
    """Generate a safe, descriptive filename for session PDFs."""
    try:
        # Parse URL to extract meaningful information
        parsed_url = urlparse(session_pdf_url)
        query_params = parse_qs(parsed_url.query)
        
        # Try to extract the actual filename from the 'Fich' parameter
        if 'Fich' in query_params:
            original_filename = query_params['Fich'][0]
            # Clean the filename and ensure it's reasonable length
            safe_filename = re.sub(r'[^\w\-_\.]', '_', original_filename)
            if len(safe_filename) > 100:  # Truncate if too long
                name_part = safe_filename[:80]
                ext_part = safe_filename[-20:] if '.' in safe_filename[-20:] else '.pdf'
                safe_filename = name_part + ext_part
        else:
            # Fallback: use a hash of the URL for uniqueness
            url_hash = hashlib.md5(session_pdf_url.encode()).hexdigest()[:8]
            safe_filename = f"session_{session_year}_{url_hash}.pdf"
        
        # Ensure .pdf extension
        if not safe_filename.lower().endswith('.pdf'):
            safe_filename += '.pdf'
            
        # Add year prefix for organization
        final_filename = f"{session_year}_{safe_filename}"
        
        return final_filename
        
    except Exception as e:
        # Ultimate fallback
        url_hash = hashlib.md5(session_pdf_url.encode()).hexdigest()[:8]
        return f"session_{session_year}_{url_hash}.pdf"


# --- Proposal Processing Function for Parallel Execution ---
def process_single_proposal(proposal_data, session_info, df, df_lock):
    """
    Process a single proposal from a session PDF.
    
    Args:
        proposal_data: Dictionary containing proposal information
        session_info: Dictionary with session metadata
        df: The DataFrame to update
        df_lock: Lock to synchronize DataFrame access
    
    Returns:
        Tuple of (row_index, updated_data) or None if skipped
    """
    proposal_name = proposal_data.get('proposal_name')
    proposal_gov_link = proposal_data.get('proposal_link')
    voting_summary = proposal_data.get('voting_summary')
    approval_status = proposal_data.get('approval_status')
    
    if not proposal_name:
        print(f"Skipping proposal with no name from {session_info['session_pdf_url']}")
        return None
        
    # Need to access df with lock to find or create the row
    with df_lock:
        match_criteria = (df['session_pdf_url'] == session_info['session_pdf_url']) & (df['proposal_name_from_session'] == proposal_name)
        if df[match_criteria].empty:
            row_idx = len(df)
            # Initialize new row with basic session information
            df.loc[row_idx, 'session_pdf_url'] = session_info['session_pdf_url']
            df.loc[row_idx, 'session_year'] = session_info['session_year']
            df.loc[row_idx, 'session_date'] = session_info['session_date']
            df.loc[row_idx, 'proposal_name_from_session'] = proposal_name
        else:
            row_idx = df[match_criteria].index[0]
    
    # Store all updates to apply at once to minimize lock time
    update_data = {
        'session_date': session_info['session_date'],
        'session_pdf_text_path': session_info['session_pdf_path'],
        'session_pdf_download_status': 'Success',
        'proposal_gov_link': proposal_gov_link,
        'voting_details_json': json.dumps(voting_summary) if voting_summary else None,
        'session_parse_status': 'Success',
        'proposal_approval_status': approval_status,
        'overall_status': 'Pending Further Stages',
        'last_error_message': None,
        'last_processed_timestamp': datetime.now().isoformat()
    }
    
    # Check if we need to perform Stage 3 (get proposal details)
    with df_lock:
        need_details_scrape = pd.isna(df.loc[row_idx, 'proposal_details_scrape_status']) or \
            df.loc[row_idx, 'proposal_details_scrape_status'] not in ['Success', 'Success (No Doc Link)', 'No Gov Link']
    
    if need_details_scrape:
        if proposal_gov_link and isinstance(proposal_gov_link, str) and proposal_gov_link.startswith("http"):
            details_result = fetch_proposal_details_and_download_doc(proposal_gov_link, PROPOSAL_DOC_DIR)
            update_data.update({
                'proposal_authors_json': details_result['authors_json'],
                'proposal_document_url': details_result['document_info']['link'],
                'proposal_document_type': details_result['document_info']['type'],
                'proposal_document_local_path': details_result['document_info']['local_path'],
                'proposal_doc_download_status': details_result['document_info']['download_status'],
                'proposal_details_scrape_status': details_result['scrape_status'],
            })
            if details_result['error']:
                update_data.update({
                    'last_error_message': details_result['error'],
                    'overall_status': 'Failed Stage 3 (Proposal Details Scrape)'
                })
        else:
            update_data.update({
                'proposal_details_scrape_status': 'No Gov Link',
                'overall_status': 'Skipped Stage 3 (No Gov Link)'
            })
    
    # Check if we need to perform Stage 4 (summarize document)
    # Get the document path from existing data or updates
    with df_lock:
        proposal_doc_path = update_data.get('proposal_document_local_path', df.loc[row_idx, 'proposal_document_local_path'])
        doc_download_status = update_data.get('proposal_doc_download_status', df.loc[row_idx, 'proposal_doc_download_status'])
        summarize_status = df.loc[row_idx, 'proposal_summarize_status']
    
    if pd.notna(proposal_doc_path) and doc_download_status == 'Success' and \
       (pd.isna(summarize_status) or summarize_status != 'Success'):
        
        # Use the PDF file path directly instead of extracting text first
        summary_data, summary_err = summarize_proposal_text(proposal_doc_path)
        if summary_err:
            update_data.update({
                'proposal_summarize_status': f'LLM Summary Failed: {summary_err}',
                'last_error_message': summary_err,
                'overall_status': 'Failed Stage 4 (LLM Summary)'
            })
        else:
            update_data.update({
                'proposal_summary_general': summary_data['general_summary'],
                'proposal_summary_analysis': summary_data['critical_analysis'],
                'proposal_summary_fiscal_impact': summary_data['fiscal_impact'],
                'proposal_summary_colloquial': summary_data['colloquial_summary'],
                'proposal_category': summary_data['categories'],
                'proposal_short_title': summary_data['short_title'],
                'proposal_proposing_party': summary_data['proposing_party'],
                'proposal_summarize_status': 'Success',
                'overall_status': 'Success'
            })
    elif details_result.get('document_info', {}).get('download_status') == 'Success' and pd.isna(proposal_doc_path):
        update_data.update({
            'proposal_summarize_status': 'Skipped - No Proposal Document',
        })
        if update_data.get('overall_status') not in ['Failed Stage 3 (Proposal Details Scrape)', 'Skipped Stage 3 (No Gov Link)']:
            update_data.update({
                'overall_status': 'Completed (No Proposal Doc to Summarize)'
            })
    
    # Update overall status if not already failed
    current_overall_status = update_data.get('overall_status', 'Pending Further Stages')
    if 'Failed' not in str(current_overall_status) and 'Skipped' not in str(current_overall_status) and current_overall_status != 'Success':
        if update_data.get('proposal_summarize_status') == 'Success':
            update_data['overall_status'] = 'Success'
        elif update_data.get('proposal_details_scrape_status') in ['Success', 'Success (No Doc Link)'] and \
             update_data.get('proposal_summarize_status') == 'Skipped - No Proposal Document':
             update_data['overall_status'] = 'Completed (No Proposal Doc to Summarize)'
        elif update_data.get('proposal_details_scrape_status') == 'No Gov Link':
             update_data['overall_status'] = 'Completed (No Gov Link for Details)'
        else:
            update_data['overall_status'] = 'Partially Processed'
    
    return row_idx, update_data

# --- Main Pipeline Orchestrator ---
def run_pipeline(start_year=None, end_year=None, max_sessions_to_process=None):
    if not GEMINI_API_KEY:
        print("Critical Error: GEMINI_API_KEY is not set. The pipeline cannot run LLM-dependent stages.")
        return

    init_directories()
    df = load_or_initialize_dataframe()

    # Stage 1: Get session PDF links
    scraper = ParliamentPDFScraper()
    current_year = datetime.now().year
    _start_year = start_year if start_year else current_year - 5 # Default to last 5 years
    _end_year = end_year if end_year else current_year
    
    print(f"--- Stage 1: Fetching Session PDF links for years {_start_year}-{_end_year} ---")
    session_pdf_infos = scraper.scrape_years(start_year=_start_year, end_year=_end_year)
    print(f"Found {len(session_pdf_infos)} potential session PDF links.")

    processed_sessions_count = 0
    for session_info in session_pdf_infos:
        if max_sessions_to_process and processed_sessions_count >= max_sessions_to_process:
            print(f"Reached max_sessions_to_process limit ({max_sessions_to_process}). Stopping.")
            break
        
        session_pdf_url = session_info['url']
        session_year = session_info['year']
        session_date = session_info.get('date')  # May be None if date extraction failed
        print(f"\nProcessing session PDF URL: {session_pdf_url} (Year: {session_year}, Date: {session_date})")

        # Check if this session PDF has already been fully processed for all its proposals
        # This is a simplified check; more robust would be per-proposal status.
        # For now, if any proposal from this session_pdf_url is not 'Success' in overall_status, re-process.
        existing_session_entries = df[df['session_pdf_url'] == session_pdf_url]
        if not existing_session_entries.empty and \
           all(status == 'Success' for status in existing_session_entries['overall_status']):
            print(f"Session PDF {session_pdf_url} already fully processed. Skipping.")
            continue

        # Download session PDF - FIXED FILENAME GENERATION
        session_pdf_filename = generate_session_pdf_filename(session_pdf_url, session_year)
        session_pdf_local_path = os.path.join(SESSION_PDF_DIR, session_pdf_filename)
        
        # Update DataFrame for this session PDF download attempt (even if it fails)
        # If multiple proposals come from one PDF, this info will be duplicated or handled by finding existing rows.
        # For simplicity, we'll update/create rows when proposals are identified.

        download_success, msg_or_path = False, "Not attempted"
        if not (not existing_session_entries.empty and existing_session_entries['session_pdf_download_status'].iloc[0] == 'Success'):
            download_success, msg_or_path = download_file(session_pdf_url, session_pdf_local_path)
        else: # Already downloaded
            if os.path.exists(existing_session_entries['session_pdf_text_path'].iloc[0]):
                 download_success, msg_or_path = True, existing_session_entries['session_pdf_text_path'].iloc[0]
                 session_pdf_local_path = msg_or_path # use existing path
                 print(f"Session PDF already downloaded: {session_pdf_local_path}")
            else: # DB says downloaded, but file missing. Redownload.
                 print(f"Session PDF was marked downloaded but file missing. Re-downloading: {session_pdf_local_path}")
                 download_success, msg_or_path = download_file(session_pdf_url, session_pdf_local_path)


        if not download_success:
            # Create a placeholder row if no proposals can be extracted
            # This ensures the session PDF URL itself is logged as failed.
            if df[(df['session_pdf_url'] == session_pdf_url)].empty:
                new_row_idx = len(df)
                df.loc[new_row_idx, 'session_pdf_url'] = session_pdf_url
                df.loc[new_row_idx, 'session_year'] = session_year
                df.loc[new_row_idx, 'session_date'] = session_date
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Download Failed'
                df.loc[new_row_idx, 'last_error_message'] = msg_or_path
                df.loc[new_row_idx, 'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else:
                idx_to_update = df[df['session_pdf_url'] == session_pdf_url].index
                df.loc[idx_to_update, 'session_pdf_download_status'] = 'Download Failed'
                df.loc[idx_to_update, 'session_date'] = session_date
                df.loc[idx_to_update, 'last_error_message'] = msg_or_path
                df.loc[idx_to_update, 'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                df.loc[idx_to_update, 'last_processed_timestamp'] = datetime.now().isoformat()
            save_dataframe(df)
            continue
        
        # Successfully downloaded (or already downloaded)
        current_session_pdf_path = session_pdf_local_path # msg_or_path holds the path if success
        
        # Stage 2: Parse session PDF with LLM (using raw bytes)
        proposals_in_session, llm_parse_error = extract_votes_from_session_pdf_text(current_session_pdf_path)
        if llm_parse_error:
            indices = df[df['session_pdf_url'] == session_pdf_url].index
            if indices.empty:
                new_row_idx = len(df)
                df.loc[new_row_idx, 'session_pdf_url'] = session_pdf_url
                df.loc[new_row_idx, 'session_year'] = session_year
                df.loc[new_row_idx, 'session_date'] = session_date
                df.loc[new_row_idx, 'session_pdf_text_path'] = current_session_pdf_path
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Success'
                df.loc[new_row_idx, 'session_parse_status'] = f'LLM Parse Failed: {llm_parse_error}'
                df.loc[new_row_idx, 'last_error_message'] = llm_parse_error
                df.loc[new_row_idx, 'overall_status'] = 'Failed Stage 2 (LLM Session Parse)'
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else:
                for idx in indices:
                    df.loc[idx, 'session_date'] = session_date
                    df.loc[idx, 'session_pdf_text_path'] = current_session_pdf_path
                    df.loc[idx, 'session_pdf_download_status'] = 'Success'
                    df.loc[idx, 'session_parse_status'] = f'LLM Parse Failed: {llm_parse_error}'
                    df.loc[idx, 'last_error_message'] = llm_parse_error
                    df.loc[idx, 'overall_status'] = 'Failed Stage 2 (LLM Session Parse)'
                    df.loc[idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            save_dataframe(df)
            continue
        
        if not proposals_in_session:
            print(f"No proposals extracted by LLM from {session_pdf_url}.")
            # Log this, but don't necessarily mark as failure of the whole session PDF if it was parsed.
            # Could be an empty voting day.
            indices = df[df['session_pdf_url'] == session_pdf_url].index
            status_message = "LLM Parsed - No Proposals Found"
            if indices.empty:
                new_row_idx = len(df)
                df.loc[new_row_idx, 'session_pdf_url'] = session_pdf_url
                df.loc[new_row_idx, 'session_year'] = session_year
                df.loc[new_row_idx, 'session_date'] = session_date
                df.loc[new_row_idx, 'session_pdf_text_path'] = current_session_pdf_path
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Success'
                df.loc[new_row_idx, 'session_parse_status'] = status_message
                df.loc[new_row_idx, 'overall_status'] = 'Completed (No Proposals)' # Or a specific status
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else:
                 for idx in indices: # Should ideally be one summary row if no proposals
                    df.loc[idx, 'session_date'] = session_date
                    df.loc[idx, 'session_pdf_text_path'] = current_session_pdf_path
                    df.loc[idx, 'session_pdf_download_status'] = 'Success'
                    df.loc[idx, 'session_parse_status'] = status_message
                    df.loc[idx, 'overall_status'] = 'Completed (No Propostas)'
                    df.loc[idx, 'last_processed_timestamp'] = datetime.now().isoformat()

            save_dataframe(df)
            processed_sessions_count += 1
            continue # Move to next session PDF

        print(f"LLM extracted {len(proposals_in_session)} proposals from {session_pdf_url}.")
        
        # Prepare session info dictionary to pass to the parallel function
        session_info = {
            'session_pdf_url': session_pdf_url,
            'session_year': session_year,
            'session_date': session_date,
            'session_pdf_path': current_session_pdf_path
        }
        
        # Create a lock for DataFrame synchronization
        df_lock = threading.Lock()
        
        # Process proposals in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all proposal processing tasks
            futures = [
                executor.submit(process_single_proposal, proposal_data, session_info, df, df_lock)
                for proposal_data in proposals_in_session
            ]
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        row_idx, update_data = result
                        # Apply updates to DataFrame with lock
                        with df_lock:
                            for col, value in update_data.items():
                                df.loc[row_idx, col] = value
                        
                        # Save after each proposal is processed to avoid data loss
                        with df_lock:
                            save_dataframe(df)
                except Exception as e:
                    print(f"Error in parallel processing: {e}")
        
        processed_sessions_count += 1

    print("\n--- Pipeline Run Finished ---")
    print(df['overall_status'].value_counts())


if __name__ == "__main__":
    # Example: Process data for the last 2 years, up to 10 session PDFs
    # For a full run, you might remove max_sessions_to_process or set it higher
    # And adjust start_year as needed.
    run_pipeline(start_year=2023, end_year=2023, max_sessions_to_process=None) 
    # To run for all available years from 2012 (as per original script 1 default):
    # run_pipeline(start_year=2012, end_year=datetime.now().year)
