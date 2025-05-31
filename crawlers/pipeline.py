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

# --- Configuration ---
DATAFRAME_PATH = "parliament_data.csv"
SESSION_PDF_DIR = "data/session_pdfs"
PROPOSAL_DOC_DIR = "data/proposal_docs"
DOWNLOAD_TIMEOUT = 60  # seconds for requests timeout
LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_DELAY = 5  # seconds

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY not found in .env file. Please create a .env file with your API key.")
    # You might want to exit here or raise an error depending on desired behavior
    # For now, we'll let it proceed, but LLM calls will fail.

# Gemini API Configuration
# GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent" # Changed from gemini-2.0-flash-lite for potentially better JSON handling
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite-001:generateContent"
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
        'session_pdf_url', 'session_year', 'session_pdf_text_path', 'session_pdf_download_status',
        'proposal_name_from_session', 'proposal_gov_link', 'voting_details_json', 'session_parse_status',
        'proposal_authors_json', 'proposal_document_url', 'proposal_document_type', 
        'proposal_document_local_path', 'proposal_doc_download_status', 'proposal_details_scrape_status',
        'proposal_summary_general', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 
        'proposal_summary_colloquial', 'proposal_category', 'proposal_summarize_status',
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

def call_gemini_api(prompt_text, document_content=None, expect_json=False):
    """Calls the Gemini API with the given prompt and optional document content."""
    if not GEMINI_API_KEY:
        return None, "GEMINI_API_KEY not configured"

    full_prompt = prompt_text
    if document_content:
        full_prompt = f"{prompt_text}\n\nDocument content:\n{document_content}"
    
    print(f"Calling Gemini API. Prompt length: {len(full_prompt)}")

    payload = {"contents": [{"parts": [{"text": full_prompt}]}]}
    if expect_json:
        # Note: gemini-2.0-flash might not fully support responseSchema.
        # This is an attempt; parsing will be robust.
        payload["generationConfig"] = {
            "responseMimeType": "application/json",
        }

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY
    }

    for attempt in range(LLM_RETRY_ATTEMPTS):
        try:
            response = requests.post(GEMINI_API_URL, json=payload, headers=headers, timeout=600)
            response.raise_for_status()
            result = response.json()
            
            if not result.get("candidates"):
                print(f"Gemini API Error: No candidates in response. Full response: {result}")
                return None, f"No candidates in API response: {result.get('error', {}).get('message', 'Unknown error')}"

            generated_text = result["candidates"][0].get("content", {}).get("parts", [{}])[0].get("text", "")
            
            if not generated_text.strip():
                 print(f"Gemini API Warning: Empty text response. Full result: {result}")
                 # Check for safety ratings or finish reason
                 finish_reason = result["candidates"][0].get("finishReason")
                 if finish_reason and finish_reason != "STOP":
                     return None, f"API call finished with reason: {finish_reason}. Safety ratings: {result['candidates'][0].get('safetyRatings')}"
                 return None, "Empty text response from API"


            if expect_json:
                # Gemini might return JSON as a string, sometimes with ```json ... ```
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
                    return None, f"JSONDecodeError: {e}. Raw text: {generated_text[:500]}" # Log part of the raw text
            
            print("Successfully received text response from Gemini API.")
            return generated_text, None

        except requests.exceptions.RequestException as e:
            print(f"Error communicating with Gemini API (attempt {attempt + 1}/{LLM_RETRY_ATTEMPTS}): {e}")
            if attempt + 1 == LLM_RETRY_ATTEMPTS:
                return None, f"RequestException after {LLM_RETRY_ATTEMPTS} attempts: {e}"
        except json.JSONDecodeError as e: # If response itself is not JSON
            print(f"Error decoding API outer response (attempt {attempt + 1}/{LLM_RETRY_ATTEMPTS}): {e}")
            if attempt + 1 == LLM_RETRY_ATTEMPTS:
                return None, f"Outer JSONDecodeError after {LLM_RETRY_ATTEMPTS} attempts: {e}"
        except Exception as e:
            print(f"An unexpected error occurred with Gemini API (attempt {attempt + 1}/{LLM_RETRY_ATTEMPTS}): {e}")
            if attempt + 1 == LLM_RETRY_ATTEMPTS:
                return None, f"Unexpected API error after {LLM_RETRY_ATTEMPTS} attempts: {e}"
        
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
        all_anchor_tags = soup.find_all('a', href=True)
        
        for link_tag in all_anchor_tags:
            href = link_tag.get('href', '')
            text_content = link_tag.get_text(strip=True)
            
            # Prioritize links that look like direct PDF links related to voting summaries
            # Example: DARxxx.pdf, _Votacoes_, _ResultadosVotacao_
            if (href.lower().endswith('.pdf') and 
                any(kw in href.lower() for kw in ['votacoe', 'resultado', 'dar', 'serieii'])): # Added serieii based on typical DAR naming
                full_url = urljoin("https://www.parlamento.pt", href)
                # Further check if text implies it's a voting summary
                if "votaç" in text_content.lower() or "diário" in text_content.lower() or "reunião plenária" in text_content.lower():
                    pdf_links.append({'url': full_url, 'year': year, 'text': text_content, 'type': 'direct_pdf_votacao'})
            # Parameterized links that often lead to PDFs
            elif ('doc.pdf' in href.lower() or 'path=' in href.lower() or 'downloadfile' in href.lower()):
                 if "votaç" in text_content.lower() or "diário" in text_content.lower():
                    full_url = urljoin("https://www.parlamento.pt", href)
                    pdf_links.append({'url': full_url, 'year': year, 'text': text_content, 'type': 'parameterized_pdf_votacao'})
        
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
def extract_votes_from_session_pdf_text(session_pdf_path):
    prompt = """This is the voting record from a parliamentary session in Portugal.
Identify all distinct issues/proposals voted on in this document.
For each issue/proposal, extract the following information:
1.  'proposal_name': The unique identifier (e.g., "Projeto de Lei 404/XVI/1", "Proposta de Lei 39/XVI/1"). This is often found near the title of the item being voted.
2.  'proposal_link': The hyperlink associated with this specific issue/proposal, if explicitly mentioned in the text. This link usually points to a parlamento.pt URL. If no link is found, use null.
3.  'voting_summary': A structured representation of how each political party voted (e.g., In Favor, Against, Abstain) and the number of votes for each. The parties are usually abbreviated (PS, PSD, CH, IL, PCP, BE, PAN, L). Capture the votes for each party. The table might show numbers or 'X' (meaning all MPs of that party voted that way, with total party MPs often at the top of the column or table).

Output the result as a JSON array, where each element of the array is an object corresponding to one proposal. Each object must contain 'proposal_name', 'proposal_link', and 'voting_summary' keys.
Example of a single element in the JSON array:
{
  "proposal_name": "Projeto de Lei 123/XV/2",
  "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=XXXXX",
  "voting_summary": {
    "PS": {"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2},
    "PSD": {"Favor": 0, "Contra": "X", "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 70},
    "CH": {"Favor": 10, "Contra": 0, "Abstenção": 0, "Não Votaram": 0}
  }
}
If you cannot find a specific piece of information for a proposal (e.g. a link), use null for its value. Ensure the output is a valid JSON array.
"""
    
    # Read PDF as raw bytes and convert to UTF-8 dump
    try:
        with open(session_pdf_path, 'rb') as f:
            pdf_raw_bytes = f.read()
        # Convert to UTF-8 string with error handling to create the "dump" effect
        pdf_utf8_dump = pdf_raw_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        return None, f"Failed to read PDF as raw bytes: {e}"
    
    extracted_data, error = call_gemini_api(prompt, document_content=pdf_utf8_dump, expect_json=True)
    if error:
        return None, f"LLM API call failed: {error}"
    if not isinstance(extracted_data, list): # Expecting a list of proposals
        return None, f"LLM did not return a list as expected. Got: {type(extracted_data)}"
    
    # Basic validation of returned structure
    valid_proposals = []
    for item in extracted_data:
        if isinstance(item, dict) and 'proposal_name' in item and 'voting_summary' in item:
            valid_proposals.append(item)
        else:
            print(f"Warning: LLM returned an invalid item structure: {item}")
            
    if not valid_proposals and extracted_data: # If some items were returned but none were valid
         return None, f"LLM returned data but no valid proposal structures found. Raw: {str(extracted_data)[:500]}"
    elif not valid_proposals and not extracted_data: # If nothing was returned or parsed
        return None, "LLM returned no processable proposal data."

    return valid_proposals, None


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
def summarize_proposal_text(proposal_document_text):
    prompt = """Provide this answer in Portuguese from Portugal: This is a government proposal that was voted on in the Portuguese Parliament in Portugal and so is full of legal language. Analyze this document and provide a structured JSON response with the following fields:

1. "general_summary": A general summary of the proposal, avoiding legalese and using normal vocabulary
2. "critical_analysis": Think critically about the document and point out inconsistencies if there are any, and if not show how the implementation details align with the goal
3. "fiscal_impact": An educated estimate if the proposal will increase or decrease government spending and increase or decrease government revenue as well, and what the net effect may be
4. "colloquial_summary": Another summary, but in more colloquial language
5. "categories": An array of one or more categories that this proposal fits into. Choose from the following categories:
   - "Saude e Cuidados Sociais"
   - "Educacao e Competências"
   - "Defesa e Segurança Nacional"
   - "Justica, Lei e Ordem"
   - "Economia e Financas"
   - "Bem-Estar e Seguranca Social"
   - "Ambiente, Agricultura e Pescas"
   - "Energia e Clima"
   - "Transportes e Infraestruturas"
   - "Habitacao, Comunidades e Administracao Local"
   - "Negocios Estrangeiros e Cooperacao Internacional"
   - "Ciencia, Tecnologia e Digital"

Return only a valid JSON object with these 5 fields. If the proposal fits multiple categories, include all relevant ones in the "categories" array.

Example format:
{
  "general_summary": "...",
  "critical_analysis": "...",
  "fiscal_impact": "...",
  "colloquial_summary": "...",
  "categories": ["Economia e Finanças", "Bem-Estar e Segurança Social"]
}
"""
    summary_data, error = call_gemini_api(prompt, document_content=proposal_document_text, expect_json=True)
    if error:
        return None, f"LLM API call failed for summary: {error}"
    
    # Validate the returned JSON structure
    if not isinstance(summary_data, dict):
        return None, f"LLM did not return a JSON object as expected. Got: {type(summary_data)}"
    
    required_fields = ['general_summary', 'critical_analysis', 'fiscal_impact', 'colloquial_summary', 'categories']
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
        print(f"\nProcessing session PDF URL: {session_pdf_url} (Year: {session_year})")

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
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Download Failed'
                df.loc[new_row_idx, 'last_error_message'] = msg_or_path
                df.loc[new_row_idx, 'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else: # Update existing placeholder if any
                idx_to_update = df[df['session_pdf_url'] == session_pdf_url].index
                df.loc[idx_to_update, 'session_pdf_download_status'] = 'Download Failed'
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
                df.loc[new_row_idx, 'session_pdf_text_path'] = current_session_pdf_path
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Success'
                df.loc[new_row_idx, 'session_parse_status'] = f'LLM Parse Failed: {llm_parse_error}'
                df.loc[new_row_idx, 'last_error_message'] = llm_parse_error
                df.loc[new_row_idx, 'overall_status'] = 'Failed Stage 2 (LLM Session Parse)'
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else:
                for idx in indices:
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
                df.loc[new_row_idx, 'session_pdf_text_path'] = current_session_pdf_path
                df.loc[new_row_idx, 'session_pdf_download_status'] = 'Success'
                df.loc[new_row_idx, 'session_parse_status'] = status_message
                df.loc[new_row_idx, 'overall_status'] = 'Completed (No Proposals)' # Or a specific status
                df.loc[new_row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            else:
                 for idx in indices: # Should ideally be one summary row if no proposals
                    df.loc[idx, 'session_pdf_text_path'] = current_session_pdf_path
                    df.loc[idx, 'session_pdf_download_status'] = 'Success'
                    df.loc[idx, 'session_parse_status'] = status_message
                    df.loc[idx, 'overall_status'] = 'Completed (No Proposals)'
                    df.loc[idx, 'last_processed_timestamp'] = datetime.now().isoformat()

            save_dataframe(df)
            processed_sessions_count += 1
            continue # Move to next session PDF

        print(f"LLM extracted {len(proposals_in_session)} proposals from {session_pdf_url}.")

        # For each proposal found in the session PDF
        for proposal_data in proposals_in_session:
            proposal_name = proposal_data.get('proposal_name')
            proposal_gov_link = proposal_data.get('proposal_link') # May be null
            voting_summary = proposal_data.get('voting_summary')

            if not proposal_name:
                print(f"Skipping proposal with no name from {session_pdf_url}")
                continue

            # Find or create row for this specific proposal
            match_criteria = (df['session_pdf_url'] == session_pdf_url) & (df['proposal_name_from_session'] == proposal_name)
            if df[match_criteria].empty:
                row_idx = len(df)
                df.loc[row_idx, 'session_pdf_url'] = session_pdf_url
                df.loc[row_idx, 'session_year'] = session_year
                df.loc[row_idx, 'proposal_name_from_session'] = proposal_name
            else:
                row_idx = df[match_criteria].index[0]

            # Update common info from session PDF processing
            df.loc[row_idx, 'session_pdf_text_path'] = current_session_pdf_path
            df.loc[row_idx, 'session_pdf_download_status'] = 'Success'
            df.loc[row_idx, 'proposal_gov_link'] = proposal_gov_link
            df.loc[row_idx, 'voting_details_json'] = json.dumps(voting_summary) if voting_summary else None
            df.loc[row_idx, 'session_parse_status'] = 'Success'
            df.loc[row_idx, 'overall_status'] = 'Pending Further Stages' # Initial status after session parse
            df.loc[row_idx, 'last_error_message'] = None # Clear previous errors for this row
            df.loc[row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()

            # Stage 3: Get proposal details (authors, actual proposal doc link)
            if pd.isna(df.loc[row_idx, 'proposal_details_scrape_status']) or \
               df.loc[row_idx, 'proposal_details_scrape_status'] not in ['Success', 'Success (No Doc Link)', 'No Gov Link']:
                if proposal_gov_link and isinstance(proposal_gov_link, str) and proposal_gov_link.startswith("http"):
                    details_result = fetch_proposal_details_and_download_doc(proposal_gov_link, PROPOSAL_DOC_DIR)
                    df.loc[row_idx, 'proposal_authors_json'] = details_result['authors_json']
                    df.loc[row_idx, 'proposal_document_url'] = details_result['document_info']['link']
                    df.loc[row_idx, 'proposal_document_type'] = details_result['document_info']['type']
                    df.loc[row_idx, 'proposal_document_local_path'] = details_result['document_info']['local_path']
                    df.loc[row_idx, 'proposal_doc_download_status'] = details_result['document_info']['download_status']
                    df.loc[row_idx, 'proposal_details_scrape_status'] = details_result['scrape_status']
                    if details_result['error']:
                        df.loc[row_idx, 'last_error_message'] = details_result['error']
                        df.loc[row_idx, 'overall_status'] = 'Failed Stage 3 (Proposal Details Scrape)'
                else:
                    df.loc[row_idx, 'proposal_details_scrape_status'] = 'No Gov Link'
                    df.loc[row_idx, 'overall_status'] = 'Skipped Stage 3 (No Gov Link)'
                df.loc[row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()

            # Stage 4: Summarize proposal document
            proposal_doc_path = df.loc[row_idx, 'proposal_document_local_path']
            if pd.notna(proposal_doc_path) and \
               df.loc[row_idx, 'proposal_doc_download_status'] == 'Success' and \
               (pd.isna(df.loc[row_idx, 'proposal_summarize_status']) or df.loc[row_idx, 'proposal_summarize_status'] != 'Success'):
                
                proposal_doc_text, prop_text_err = extract_text_from_pdf(proposal_doc_path)
                if prop_text_err:
                    df.loc[row_idx, 'proposal_summarize_status'] = f'Text Extraction Failed: {prop_text_err}'
                    df.loc[row_idx, 'last_error_message'] = prop_text_err
                    df.loc[row_idx, 'overall_status'] = 'Failed Stage 4 (Proposal Text Extraction)'
                else:
                    summary_data, summary_err = summarize_proposal_text(proposal_doc_text)
                    if summary_err:
                        df.loc[row_idx, 'proposal_summarize_status'] = f'LLM Summary Failed: {summary_err}'
                        df.loc[row_idx, 'last_error_message'] = summary_err
                        df.loc[row_idx, 'overall_status'] = 'Failed Stage 4 (LLM Summary)'
                    else:
                        # Store individual summary fields in separate columns
                        df.loc[row_idx, 'proposal_summary_general'] = summary_data['general_summary']
                        df.loc[row_idx, 'proposal_summary_analysis'] = summary_data['critical_analysis']
                        df.loc[row_idx, 'proposal_summary_fiscal_impact'] = summary_data['fiscal_impact']
                        df.loc[row_idx, 'proposal_summary_colloquial'] = summary_data['colloquial_summary']
                        df.loc[row_idx, 'proposal_category'] = summary_data['categories']  # Now stores JSON array as string
                        df.loc[row_idx, 'proposal_summarize_status'] = 'Success'
                        df.loc[row_idx, 'overall_status'] = 'Success' # Final success for this proposal
                df.loc[row_idx, 'last_processed_timestamp'] = datetime.now().isoformat()
            elif df.loc[row_idx, 'proposal_details_scrape_status'] == 'Success' and pd.isna(proposal_doc_path):
                 # Scraped details, but no proposal doc was found/downloaded
                 df.loc[row_idx, 'proposal_summarize_status'] = 'Skipped - No Proposal Document'
                 if df.loc[row_idx, 'overall_status'] not in ['Failed Stage 3 (Proposal Details Scrape)', 'Skipped Stage 3 (No Gov Link)']:
                    df.loc[row_idx, 'overall_status'] = 'Completed (No Proposal Doc to Summarize)'


            # Update overall status if not already failed
            current_overall_status = df.loc[row_idx, 'overall_status']
            if 'Failed' not in str(current_overall_status) and 'Skipped' not in str(current_overall_status) and current_overall_status != 'Success':
                if df.loc[row_idx, 'proposal_summarize_status'] == 'Success':
                    df.loc[row_idx, 'overall_status'] = 'Success'
                elif df.loc[row_idx, 'proposal_details_scrape_status'] in ['Success', 'Success (No Doc Link)'] and \
                     df.loc[row_idx, 'proposal_summarize_status'] == 'Skipped - No Proposal Document':
                     df.loc[row_idx, 'overall_status'] = 'Completed (No Proposal Doc to Summarize)'
                elif df.loc[row_idx, 'proposal_details_scrape_status'] == 'No Gov Link':
                     df.loc[row_idx, 'overall_status'] = 'Completed (No Gov Link for Details)'
                else: # If some intermediate stage is done but not all
                    df.loc[row_idx, 'overall_status'] = 'Partially Processed'


            save_dataframe(df) # Save after each proposal is processed
        
        processed_sessions_count += 1

    print("\n--- Pipeline Run Finished ---")
    print(df['overall_status'].value_counts())


if __name__ == "__main__":
    # Example: Process data for the last 2 years, up to 10 session PDFs
    # For a full run, you might remove max_sessions_to_process or set it higher
    # And adjust start_year as needed.
    run_pipeline(start_year=datetime.now().year, end_year=datetime.now().year, max_sessions_to_process=None) 
    # To run for all available years from 2012 (as per original script 1 default):
    # run_pipeline(start_year=2012, end_year=datetime.now().year)
