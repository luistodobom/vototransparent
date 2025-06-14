import time
import os
import json
import re
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from config import *
from utils import *


# --- Script 1: Get PDFs (Session Summaries) ---
class ParliamentPDFScraper:
    def __init__(self):
        self.base_url = "https://www.parlamento.pt/ArquivoDocumentacao/Paginas/Arquivodevotacoes.aspx"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_page_content(self, year=None):
        url = f"{self.base_url}?ano={year}" if year else self.base_url
        print(f"Fetching session list for year: {year if year else 'current'}")
        
        response, error = http_request_with_retry(url, headers=self.session.headers, timeout=DOWNLOAD_TIMEOUT)
        
        if error:
            print(f"Error fetching page for year {year}: {error}")
            return None
        
        return response.text

    def extract_pdf_links_from_html(self, html_content, year):
        if not html_content:
            return []
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []

        # Find all calendar detail containers that contain both date and PDF links
        calendar_details = soup.find_all(
            'div', class_='row home_calendar hc-detail')

        for calendar_detail in calendar_details:
            # Extract date information
            date_elem = calendar_detail.find('p', class_='date')
            time_elem = calendar_detail.find('p', class_='time')

            session_date = None
            year_text_from_time_elem = None  # Store year from time element
            if date_elem and time_elem:
                try:
                    day_month = date_elem.get_text(strip=True)  # e.g., "19.12"
                    year_text_from_time_elem = time_elem.get_text(
                        strip=True)  # e.g., "2024"

                    if '.' in day_month and year_text_from_time_elem.isdigit():
                        day, month = day_month.split('.')
                        # Convert to ISO date format (YYYY-MM-DD)
                        session_date = f"{year_text_from_time_elem}-{month.zfill(2)}-{day.zfill(2)}"
                except (ValueError, AttributeError) as e:
                    print(
                        f"Error parsing date from {day_month} and {year_text_from_time_elem}: {e}")
                    session_date = None  # Ensure session_date is None on error

            # Find PDF links within this calendar detail
            all_anchor_tags = calendar_detail.find_all('a', href=True)

            for link_tag in all_anchor_tags:
                href = link_tag.get('href', '')
                text_content = link_tag.get_text(strip=True)

                # Skip supplementary guides
                if "guião suplementar" in text_content.lower():
                    print(
                        f"Skipping supplementary guide: {text_content} ({href})")
                    continue

                # Determine year for this link: use parsed year_text_from_time_elem if available, else fallback to function's year param
                link_year = int(
                    year_text_from_time_elem) if year_text_from_time_elem and year_text_from_time_elem.isdigit() else year

                # Prioritize links that look like direct PDF links related to voting summaries
                if (href.lower().endswith('.pdf') and
                        any(kw in href.lower() for kw in ['votacoe', 'resultado', 'dar', 'serieii'])):
                    full_url = urljoin("https://www.parlamento.pt", href)
                    if "votaç" in text_content.lower() or "diário" in text_content.lower() or "reunião plenária" in text_content.lower():
                        pdf_links.append({
                            'url': full_url,
                            'year': link_year,
                            'date': session_date,  # Use parsed date if available for this calendar_detail
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
                            'date': session_date,  # Use parsed date if available for this calendar_detail
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

        print(
            f"Found {len(unique_links)} potential session PDF links for year {year}")
        return unique_links

    def scrape_years(self, start_year, end_year):
        print(f"Scraping session PDF links from {start_year} to {end_year}")
        all_pdf_links = []
        for year_to_scrape in range(start_year, end_year + 1):
            html_content = self.get_page_content(year_to_scrape)
            if html_content:
                year_links = self.extract_pdf_links_from_html(
                    html_content, year_to_scrape)
                all_pdf_links.extend(year_links)
            time.sleep(1)
        return all_pdf_links


def fetch_proposal_details_and_download_doc(proposal_page_url, download_dir):
    """
    Fetches author info and document link from proposal_page_url.
    Downloads the document if it's a PDF.
    """
    authors_list = []
    document_info = {'link': None, 'type': None, 'local_path': None,
                     'download_status': 'Not Attempted', 'error': None}
    scrape_status = 'Pending'

    print(f"Fetching proposal details from: {proposal_page_url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response, error = http_request_with_retry(proposal_page_url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
    
    if error:
        print(f"Error fetching URL {proposal_page_url}: {error}")
        return {'authors_json': None, 'document_info': document_info, 'scrape_status': 'Fetch Failed', 'error': error}
    
    html_content = response.text

    soup = BeautifulSoup(html_content, 'lxml')
    base_url = f"{urlparse(proposal_page_url).scheme}://{urlparse(proposal_page_url).netloc}"

    autoria_heading = soup.find(lambda tag: tag.name == "div" and "Autoria" in tag.get_text(
        strip=True) and "Titulo-Cinzento" in tag.get("class", []))
    if autoria_heading:
        autoria_section_container = autoria_heading.find_parent('div')
        if autoria_section_container:
            authors_div = autoria_section_container.find_next_sibling('div')
            if authors_div:
                author_links_tags = authors_div.find_all(
                    'a', class_='LinksTram')
                for link_tag in author_links_tags:
                    name = link_tag.get_text(strip=True)
                    href = link_tag.get('href')
                    if name and href:
                        authors_list.append(
                            {'name': name, 'link': urljoin(base_url, href)})

    authors_json = json.dumps(authors_list) if authors_list else None

    doc_search_priority = [
        ('PDF', [lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoPDF')),
                 lambda s: s.find(
                     'a', string=lambda t: t and '[formato PDF]' in t.strip().lower()),
                 lambda s: next((tag for tag in s.find_all('a', href=True) if '.pdf' in tag.get('href', '').lower() and any(kw in tag.get_text(strip=True).lower() for kw in ['pdf', 'documento', 'ficheiro', 'texto integral', 'texto final'])), None)]),
        ('DOCX', [lambda s: next((tag for tag in s.find_all('a', href=True) if '.docx' in tag.get('href', '').lower(
        ) and any(kw in tag.get_text(strip=True).lower() for kw in ['docx', 'documento', 'word'])), None)]),
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
                bid_value = bid_match.group(1) if bid_match else hashlib.md5(
                    proposal_page_url.encode()).hexdigest()[:8]

                doc_link_text = found_doc_link_tag.get_text(strip=True)
                sane_link_text = re.sub(r'[^\w\s-]', '', doc_link_text).strip()
                sane_link_text = re.sub(r'[-\s]+', '_', sane_link_text)[:50]

                file_name = f"proposal_{bid_value}_{sane_link_text}.pdf" if sane_link_text else f"proposal_{bid_value}.pdf"
                file_name = re.sub(r'_+', '_', file_name)

                local_path = os.path.join(download_dir, file_name)

                success, msg_or_path = download_file(
                    doc_url, local_path, is_pdf=True)
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
