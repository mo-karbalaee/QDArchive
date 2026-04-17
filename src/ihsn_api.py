import requests
from models.person_role import PersonRole 
import re
from langdetect import detect
from bs4 import BeautifulSoup
from keybert import KeyBERT
import os
import logging
import transformers

transformers.logging.set_verbosity_error()
transformers.logging.disable_progress_bar()
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3" 
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

class IhsnApi:
    def __init__(self, base_url, api_key):
        self.api_url = base_url.rstrip('/')
        self.site_base = self.api_url.split('/index.php')[0]
        self.headers = {
            "X-API-Key": api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.downloaded_in_session = set()
        self.kw_model = KeyBERT()

    def search_datasets(self, query, limit=5):
        url = f"{self.api_url}/catalog/search"
        params = {"sk": query, "ps": limit}
        try:
            response = requests.get(url, headers=self.headers, params=params)
            return response.json().get('result', {}).get('rows', [])
        except Exception:
            return []
        
    def get_full_metadata(self, internal_id):
        """Simple URL fetch for the JSON export + Scrape for download links."""
        url = f"{self.site_base}/metadata/export/{internal_id}/json"
        scrape_url = f"{self.site_base}/catalog/{internal_id}/related-materials"
        
        data = {}
        try:
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()

            # Scraping for download links
            scraped_files = []
            seen_urls = set()
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
                data['scraped_files_list'] = scraped_files

            return data
                
        except Exception:
            return {}

    def download_file(self, file_url, save_path):
        """Downloads the file from the generated catalog URL."""
        if file_url in self.downloaded_in_session or save_path.exists():
            self.downloaded_in_session.add(file_url)
            return
            
        try:
            response = requests.get(file_url, headers=self.headers, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=16384):
                    if chunk:
                        f.write(chunk)
            
            self.downloaded_in_session.add(file_url) 
            
        except Exception as e:
            raise e

    def parse_metadata(self, search_item, raw_json, query_string):
        """Surgically extracts fields based on the provided JSON tree structure."""
        
        doc_desc = raw_json.get('doc_desc', {})
        study_desc = raw_json.get('study_desc', {})
        study_info = study_desc.get('study_info', {})
        data_access = study_desc.get('data_access', {})
        dataset_use = data_access.get('dataset_use', {})
        
        internal_id = search_item.get('id')
        title_stmt = study_desc.get('title_statement', {})
        idno = title_stmt.get('idno')
        
        version_val = doc_desc.get('version_statement', {}).get('version')
        match = re.search(r'(?i)\b(?:version|v)\s*0*(\d+)\b', version_val)
        version = float(match.group(1)) if match else None
        cit_req = dataset_use.get('cit_req', "not found")
        match = re.search(r"https://doi\.org/\S+", cit_req)
        doi_url = match.group(0) if match else None

        project_info = {
            "query_string": query_string,
            "repository_id": 9,
            "repository_url": self.site_base,
            "project_url": f"{self.site_base}/catalog/{internal_id}",
            "version": version,
            "title": title_stmt.get('title'),
            "description": study_info.get('abstract'),
            "language": detect(study_info.get('abstract')),
            "doi": doi_url,
            "upload_date": doc_desc.get('prod_date'),
            "download_repository_folder": "ihsn",
            "download_project_folder": str(idno or internal_id).replace("/", "_"),
            "download_version_folder": f"v{version}" if version else None,
            "download_method": "API-CALL"
        }

        # People & Roles
        people = []
        for auth in study_desc.get('authoring_entity', []):
            if auth.get('name'):
                people.append({"name": auth['name'], "role": PersonRole.AUTHOR.name})
        
        for prod in doc_desc.get('producers', []):
            if prod.get('name'):
                people.append({"name": prod['name'], "role": PersonRole.OWNER.name})

        for fund in study_desc.get('production_statement', {}).get('funding_agencies', []):
            if fund.get('name'):
                people.append({"name": fund['name'], "role": PersonRole.OWNER.name})

        for dist_contact in study_desc.get('distribution_statement', {}).get('contact', []):
            if dist_contact.get('name'):
                people.append({"name": dist_contact['name'], "role": PersonRole.UPLOADER.name})

        # Keywords
        keywords = []
        notes_str = study_info.get('notes')

        keywords_extracted = self.kw_model.extract_keywords(
            (notes_str or "") + (study_info.get('abstract') or "")
        )    
        
        if notes_str:
            keywords.extend([word for word, score in keywords_extracted])

        if study_info.get('data_kind'):
            keywords.append(study_info['data_kind'])
        
        # Files
        files = []
        for f in raw_json.get('scraped_files_list', []):
            f_name = f['name']
            clean_name = "".join([c for c in f_name if c.isalnum() or c in "._- "]).strip()
            if "." not in clean_name:
                clean_name += ".pdf"
                
            files.append({
                "id": f['download_url'],
                "name": clean_name,
                "type": clean_name.split('.')[-1].lower(),
                "status": None
            })

        # Licenses
        licenses = [dataset_use.get('conditions', None)]

        return project_info, files, list(set(keywords)), people, licenses