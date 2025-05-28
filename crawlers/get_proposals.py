import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

def extract_author_info_from_url(page_url):
    """
    Fetches and parses HTML content from a given URL to extract author names,
    their profile links from the 'Autoria' section, and a link to a primary
    document (e.g., PDF, DOCX).

    Args:
        page_url (str): The URL of the webpage to crawl.

    Returns:
        str: A JSON string representing a dictionary with two keys:
             'authors': A list of dictionaries, where each dictionary
                        contains 'name' and 'link' for an author.
             'document': A dictionary with 'link' (URL to the document)
                         and 'type' (e.g., 'PDF', 'DOCX').
                         Values will be None if no document is found.
             Returns an empty author list and no document if the page
             cannot be fetched or relevant sections are not present.
    """
    authors_list = []
    document_info = {'link': None, 'type': None}
    
    default_empty_result = {
        'authors': authors_list,
        'document': document_info
    }

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(page_url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
        html_content = response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {page_url}: {e}")
        return json.dumps(default_empty_result, indent=4, ensure_ascii=False)

    soup = BeautifulSoup(html_content, 'lxml')
    
    # Determine base URL for resolving relative links
    parsed_url = urlparse(page_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

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
                        full_link = urljoin(base_url, href)
                        authors_list.append({'name': name, 'link': full_link})

    # Extract Document Link
    # Define search attempts in order of preference for document types
    # Each entry: (type_name, list_of_search_lambdas for soup)
    doc_search_priority = [
        ('PDF', [
            lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoPDF')),
            lambda s: s.find('a', string=lambda t: t and '[formato PDF]' in t.strip()),
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.pdf' in tag.get('href','').lower() and \
                            ('[formato pdf]' in tag.get_text(strip=True).lower() or 'pdf' in tag.get('title','').lower() or 'documento' in tag.get_text(strip=True).lower() or 'ficheiro' in tag.get_text(strip=True).lower())), None)
        ]),
        ('DOCX', [
            lambda s: s.find('a', id=lambda x: x and x.endswith('_hplDocumentoDOC')), # Example ID for DOCX
            lambda s: s.find('a', string=lambda t: t and '[formato DOCX]' in t.strip()),
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.docx' in tag.get('href','').lower() and \
                            ('[formato docx]' in tag.get_text(strip=True).lower() or 'docx' in tag.get('title','').lower() or 'documento' in tag.get_text(strip=True).lower() or 'ficheiro' in tag.get_text(strip=True).lower())), None)
        ]),
        ('DOC', [
            lambda s: s.find('a', string=lambda t: t and '[formato DOC]' in t.strip() and '[formato DOCX]' not in t.strip()),
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.doc' in tag.get('href','').lower() and '.docx' not in tag.get('href','').lower() and \
                            ('[formato doc]' in tag.get_text(strip=True).lower() or 'doc' in tag.get('title','').lower() or 'documento' in tag.get_text(strip=True).lower() or 'ficheiro' in tag.get_text(strip=True).lower())), None)
        ]),
        ('XLSX', [
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.xlsx' in tag.get('href','').lower() and \
                            ('[formato xlsx]' in tag.get_text(strip=True).lower() or 'excel' in tag.get_text(strip=True).lower() or 'xlsx' in tag.get('title','').lower() or 'folha de cálculo' in tag.get_text(strip=True).lower())), None)
        ]),
        ('XLS', [
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.xls' in tag.get('href','').lower() and '.xlsx' not in tag.get('href','').lower() and \
                            ('[formato xls]' in tag.get_text(strip=True).lower() or 'excel' in tag.get_text(strip=True).lower() or 'xls' in tag.get('title','').lower() or 'folha de cálculo' in tag.get_text(strip=True).lower())), None)
        ]),
        ('PPTX', [
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.pptx' in tag.get('href','').lower() and \
                            ('[formato pptx]' in tag.get_text(strip=True).lower() or 'powerpoint' in tag.get_text(strip=True).lower() or 'pptx' in tag.get('title','').lower() or 'apresentação' in tag.get_text(strip=True).lower())), None)
        ]),
        ('PPT', [
            lambda s: next((tag for tag in s.find_all('a', href=True) if '.ppt' in tag.get('href','').lower() and '.pptx' not in tag.get('href','').lower() and \
                            ('[formato ppt]' in tag.get_text(strip=True).lower() or 'powerpoint' in tag.get_text(strip=True).lower() or 'ppt' in tag.get('title','').lower() or 'apresentação' in tag.get_text(strip=True).lower())), None)
        ]),
    ]

    for doc_type, search_methods in doc_search_priority:
        link_tag = None
        for method in search_methods:
            tag = method(soup)
            if tag and tag.get('href'):
                link_tag = tag
                break 
        
        if link_tag:
            document_info['link'] = urljoin(base_url, link_tag.get('href'))
            document_info['type'] = doc_type
            break # Found a document, stop searching for other types

    page_data = {
        'authors': authors_list,
        'document': document_info
    }

    return json.dumps(page_data, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    example_url = "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=314470"
    
    print(f"Attempting to extract author info from: {example_url}")
    json_output = extract_author_info_from_url(example_url)
    print(json_output)

    # Parse the JSON output
    data = json.loads(json_output)
    
    document_link = data.get('document', {}).get('link')
    document_type = data.get('document', {}).get('type')

    if document_link and document_type == 'PDF':
        try:
            # Extract BID from the URL to use as filename
            parsed_url_obj = urlparse(example_url) # urlparse() is the imported function
            query_params = parse_qs(parsed_url_obj.query) # Use the imported parse_qs() function
            bid_value = query_params.get('BID', [None])[0]

            if bid_value:
                file_name = f"{bid_value}.pdf"
                file_path = f"data/{file_name}"

                print(f"Downloading PDF from: {document_link} to {file_path}")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                pdf_response = requests.get(document_link, headers=headers, timeout=30, stream=True)
                pdf_response.raise_for_status() # Check for download errors

                with open(file_path, 'wb') as f:
                    for chunk in pdf_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Successfully downloaded {file_name} to {file_path}")
            else:
                print("Could not extract BID from URL to create a filename.")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading PDF: {e}")
        except IOError as e:
            print(f"Error saving PDF file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during PDF download: {e}")
    elif document_link:
        print(f"Document found is not a PDF (type: {document_type}). Skipping download.")
    else:
        print("No document link found in the extracted data. Skipping download.")