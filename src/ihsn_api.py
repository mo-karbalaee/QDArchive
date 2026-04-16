import requests
from bs4 import BeautifulSoup

class IhsnApi:
    def __init__(self, base_url, api_key):
        self.api_url = base_url.rstrip('/')
        self.site_base = self.api_url.split('/index.php')[0]
        self.headers = {
            "X-API-Key": api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.downloaded_in_session = set()

    def search_datasets(self, query, limit=5):
        url = f"{self.api_url}/catalog/search"
        params = {"sk": query, "ps": limit}
        try:
            response = requests.get(url, headers=self.headers, params=params)
            return response.json().get('result', {}).get('rows', [])
        except Exception:
            return []

    def get_full_metadata(self, idno_or_id):
        api_url = f"{self.api_url}/catalog/{idno_or_id}"
        raw_data = {}
        try:
            resp = requests.get(api_url, headers=self.headers)
            raw_data = resp.json()
        except Exception:
            pass

        internal_id = raw_data.get('dataset', {}).get('id') or idno_or_id
        scrape_url = f"{self.site_base}/catalog/{internal_id}/related-materials"
        
        scraped_files = []
        seen_urls = set()
        
        try:
            s_resp = requests.get(scrape_url, headers=self.headers, timeout=15)
            if s_resp.status_code == 200:
                soup = BeautifulSoup(s_resp.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if "/download/" in href:
                        full_url = href if href.startswith('http') else f"{self.site_base}{href}"
                        if full_url not in seen_urls:
                            title = a.get('title') or a.text.strip()
                            scraped_files.append({"download_url": full_url, "name": title})
                            seen_urls.add(full_url)
                
                raw_data['scraped_files'] = scraped_files
        except Exception:
            pass

        return raw_data

    def download_file(self, file_url, save_path):
        if file_url in self.downloaded_in_session:
            return

        if save_path.exists():
            self.downloaded_in_session.add(file_url)
            return

        try:
            response = requests.get(file_url, headers=self.headers, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=16384):
                    f.write(chunk)
            
            self.downloaded_in_session.add(file_url) 
            
        except Exception:
            pass

    def parse_metadata(self, search_item, raw_data, query_string):
        """Maps NADA JSON to the SQLite Schema format with repository_id 8."""
        dataset_data = raw_data.get('dataset', {})
        idno = search_item.get("idno")
        internal_id = search_item.get("id") or dataset_data.get("id")
        
        metadata = dataset_data.get('metadata', {})
        # NADA stores citation info deep in the survey_description
        survey_desc = metadata.get('survey_description', {})
        citation = survey_desc.get('citation', {}) if isinstance(survey_desc, dict) else metadata.get('citation', {})

        project_info = {
            "query_string": query_string,
            "repository_id": 9,  
            "repository_url": self.site_base,
            "project_url": f"{self.site_base}/catalog/{internal_id}",
            "version": citation.get('version', '1.0') if isinstance(citation, dict) else '1.0',
            "title": search_item.get("title") or "Unknown Title",
            "description": search_item.get("abstract") or "No description provided.",
            "language": None, 
            "doi": citation.get('doi') if isinstance(citation, dict) else None,
            "upload_date": search_item.get("changed", "").split("T")[0], 
            "download_repository_folder": "ihsn",
            "download_project_folder": str(idno).replace("/", "_") if idno else str(internal_id),
            "download_version_folder": "v1",
            "download_method": "API-CALL"
        }

        files = []
        for f in raw_data.get('scraped_files', []):
            name = f['name']
            clean_name = "".join([c for c in name if c.isalnum() or c in "._- "]).strip()
            if "." not in clean_name:
                clean_name += ".pdf"

            files.append({
                "id": f['download_url'],
                "name": clean_name,
                "type": clean_name.split('.')[-1].lower(),
                "status": "SUCCEEDED" # Required by schema
            })

        # Try to extract keywords from tags
        keywords = [tag.get('tag') for tag in dataset_data.get('tags', []) if isinstance(tag, dict)]

        return project_info, files, keywords, [], []