import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime

class ParliamentPDFScraper:
    def __init__(self):
        self.base_url = "https://www.parlamento.pt/ArquivoDocumentacao/Paginas/Arquivodevotacoes.aspx"
        self.session = requests.Session()
        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.pdf_links = []
    
    def get_page_content(self, year=None):
        """Get the HTML content for a specific year or current year"""
        try:
            if year:
                url = f"{self.base_url}?ano={year}"
            else:
                url = self.base_url
            
            print(f"Fetching content for year: {year if year else 'current'}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page for year {year}: {e}")
            return None
    
    def extract_pdf_links(self, html_content, year):
        """Extract all PDF links from the HTML content"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Look for all links that point to PDF files
        # This includes both direct PDF links and links that lead to PDFs
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            
            # Check for direct PDF links
            if href.lower().endswith('.pdf'):
                full_url = urljoin("https://www.parlamento.pt", href)
                pdf_links.append({
                    'url': full_url,
                    'year': year,
                    'text': link.get_text(strip=True),
                    'type': 'direct_pdf'
                })
            
            # Check for links that might lead to PDFs (like the example you provided)
            elif 'doc.pdf' in href.lower() or 'path=' in href.lower():
                if href.startswith('http'):
                    full_url = href
                else:
                    full_url = urljoin("https://www.parlamento.pt", href)
                pdf_links.append({
                    'url': full_url,
                    'year': year,
                    'text': link.get_text(strip=True),
                    'type': 'parameterized_pdf'
                })
            
            # Check for any other links that might contain PDF references
            elif re.search(r'\.pdf|votac|resultado', href.lower()):
                if href.startswith('http'):
                    full_url = href
                else:
                    full_url = urljoin("https://www.parlamento.pt", href)
                pdf_links.append({
                    'url': full_url,
                    'year': year,
                    'text': link.get_text(strip=True),
                    'type': 'potential_pdf'
                })
        
        # Also look for any JavaScript or other embedded links
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                # Look for PDF URLs in JavaScript
                pdf_matches = re.findall(r'https?://[^\s"\']+\.pdf[^\s"\']*', script.string)
                for match in pdf_matches:
                    pdf_links.append({
                        'url': match,
                        'year': year,
                        'text': 'Found in JavaScript',
                        'type': 'javascript_pdf'
                    })
        
        return pdf_links
    
    def scrape_all_years(self, start_year=2012, end_year=None):
        """Scrape PDF links for all years from start_year to end_year"""
        if end_year is None:
            end_year = datetime.now().year
        
        print(f"Scraping PDF links from {start_year} to {end_year}")
        
        all_pdf_links = []
        
        for year in range(start_year, end_year + 1):
            print(f"\n--- Processing year {year} ---")
            
            html_content = self.get_page_content(year)
            if html_content:
                year_links = self.extract_pdf_links(html_content, year)
                all_pdf_links.extend(year_links)
                print(f"Found {len(year_links)} PDF links for year {year}")
            else:
                print(f"Failed to fetch content for year {year}")
            
            # Add a small delay to be respectful to the server
            time.sleep(1)
        
        self.pdf_links = all_pdf_links
        return all_pdf_links
    
    def save_links_to_file(self, filename="parliament_pdf_links.txt"):
        """Save all PDF links to a text file"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Portuguese Parliament PDF Links\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total links found: {len(self.pdf_links)}\n")
            f.write("=" * 80 + "\n\n")
            
            # Group by year
            years = sorted(set(link['year'] for link in self.pdf_links))
            
            for year in years:
                year_links = [link for link in self.pdf_links if link['year'] == year]
                f.write(f"YEAR {year} ({len(year_links)} links)\n")
                f.write("-" * 40 + "\n")
                
                for link in year_links:
                    f.write(f"URL: {link['url']}\n")
                    f.write(f"Text: {link['text']}\n")
                    f.write(f"Type: {link['type']}\n")
                    f.write("\n")
                
                f.write("\n")
        
        print(f"Links saved to {filename}")
    
    def print_summary(self):
        """Print a summary of found links"""
        if not self.pdf_links:
            print("No PDF links found.")
            return
        
        print(f"\n=== SUMMARY ===")
        print(f"Total PDF links found: {len(self.pdf_links)}")
        
        # Count by year
        year_counts = {}
        for link in self.pdf_links:
            year = link['year']
            year_counts[year] = year_counts.get(year, 0) + 1
        
        print("\nLinks by year:")
        for year in sorted(year_counts.keys()):
            print(f"  {year}: {year_counts[year]} links")
        
        # Count by type
        type_counts = {}
        for link in self.pdf_links:
            link_type = link['type']
            type_counts[link_type] = type_counts.get(link_type, 0) + 1
        
        print("\nLinks by type:")
        for link_type in sorted(type_counts.keys()):
            print(f"  {link_type}: {type_counts[link_type]} links")

def main():
    scraper = ParliamentPDFScraper()
    
    # Scrape all years from 2012 to current year
    pdf_links = scraper.scrape_all_years(2025)
    
    # Print summary
    scraper.print_summary()
    
    # Save to file
    scraper.save_links_to_file()
    
    # Print first few links as examples
    if pdf_links:
        print(f"\n=== FIRST 5 LINKS (EXAMPLES) ===")
        for i, link in enumerate(pdf_links[:5]):
            print(f"{i+1}. {link['url']}")
    
    return pdf_links

if __name__ == "__main__":
    # Install required packages first:
    # pip install requests beautifulsoup4
    
    try:
        links = main()
        print(f"\nScript completed successfully!")
        print(f"Found {len(links)} total PDF links")
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
    except Exception as e:
        print(f"An error occurred: {e}")