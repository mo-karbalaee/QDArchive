import requests
from bs4 import BeautifulSoup

class IhsnApi:
    def __init__(self, base_url, api_key):
        """
        Initializes the IHSN client. 
        base_url: The API endpoint (e.g., .../api)
        """
        self.api_url = base_url.rstrip('/')
        # Extract the site root for scraping (e.g., https://catalog.ihsn.org)
        self.site_base = self.api_url.split('/index.php')[0]
        self.headers = {
            "X-API-Key": api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    def search_datasets(self, query, limit=5):
        """Still uses API for search as it's efficient."""
        url = f"{self.api_url}/catalog/search"
        params = {"sk": query, "ps": limit}
        try:
            response = requests.get(url, headers=self.headers, params=params)
            return response.json().get('result', {}).get('rows', [])
        except Exception as e:
            print(f"ERROR [Search]: {e}")
            return []

    def get_full_metadata(self, idno_or_id):
        """
        1. Hits API for the core metadata (titles, abstract).
        2. SCRAPES the website for the actual download links.
        """
        # --- API PART: Metadata Only ---
        api_url = f"{self.api_url}/catalog/{idno_or_id}"
        raw_data = {}
        try:
            resp = requests.get(api_url, headers=self.headers)
            raw_data = resp.json()
        except Exception as e:
            print(f"ERROR [Metadata API]: {e}")

        # --- SCRAPING PART: File Discovery ---
        # We need the numeric ID for the URL. 
        # If idno_or_id is a string, we pull the numeric ID from the API response.
        internal_id = raw_data.get('dataset', {}).get('id') or idno_or_id
        scrape_url = f"{self.site_base}/catalog/{internal_id}/related-materials"
        
        print(f"🔍 Scraping download links from: {scrape_url}")
        scraped_files = []
        
        try:
            s_resp = requests.get(scrape_url, headers=self.headers, timeout=15)
            if s_resp.status_code == 200:
                soup = BeautifulSoup(s_resp.text, 'html.parser')
                # Find all links containing '/download/'
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if "/download/" in href:
                        # Grab the human-readable text as the name
                        title = a.get('title') or a.text.strip()
                        # The URL itself is the 'id' for the download_file method
                        full_url = href if href.startswith('http') else f"{self.site_base}{href}"
                        
                        scraped_files.append({
                            "download_url": full_url,
                            "name": title
                        })
                
                # Attach to raw_data so parse_metadata can find them
                raw_data['scraped_files'] = scraped_files
        except Exception as e:
            print(f"ERROR [Scraping]: {e}")

        return raw_data

    def download_file(self, file_url, save_path):
        """
        Downloads directly using the scraped URL.
        file_url: The full URL found during scraping.
        """
        try:
            # We use the same headers (includes API key in case of session requirements)
            response = requests.get(file_url, headers=self.headers, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=16384):
                    f.write(chunk)
            print(f"  └─ ✅ Downloaded: {save_path.name}")
        except Exception as e:
            print(f"  └─ ❌ Failed: {e}")

    def parse_metadata(self, search_item, raw_data, query_string):
        """Maps data and prepares the file list from the SCRAPED results."""
        dataset_data = raw_data.get('dataset', {})
        idno = search_item.get("idno")
        internal_id = search_item.get("id") or dataset_data.get("id")
        
        metadata = dataset_data.get('metadata', {})
        citation = metadata.get('survey_description', {}).get('citation', {}) or metadata.get('citation', {})

        project_info = {
            "query_string": query_string,
            "repository_url": self.site_base,
            "project_url": f"{self.site_base}/catalog/{internal_id}",
            "version": citation.get('version', '1.0') if isinstance(citation, dict) else '1.0',
            "title": search_item.get("title") or "Unknown Title",
            "description": search_item.get("abstract") or "No description provided.",
            "language": "en",
            "download_repository_folder": "ihsn",
            "download_project_folder": str(idno).replace("/", "_") if idno else str(internal_id),
            "download_version_folder": "v1",
            "download_method": "WEB-SCRAPE"
        }

        # Use the SCRAPED files
        files = []
        for f in raw_data.get('scraped_files', []):
            name = f['name']
            # Clean name for filesystem
            clean_name = "".join([c for c in name if c.isalnum() or c in "._- "]).strip()
            if "." not in clean_name:
                clean_name += ".pdf"

            files.append({
                "id": f['download_url'], # Pass the full URL as the ID to download_file
                "name": clean_name,
                "type": clean_name.split('.')[-1].lower()
            })

        return project_info, files, [], [], []