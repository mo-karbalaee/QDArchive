import requests
import json

class IhsnApi:
    def __init__(self, base_url, api_key):
        """
        Initializes the IHSN (NADA) API client.
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {"X-API-Key": api_key}
        print(f"DEBUG [Init]: Base URL set to {self.base_url}")

    def search_datasets(self, query, limit=5):
        """Searches for studies in the IHSN catalog."""
        url = f"{self.base_url}/catalog/search"
        params = {"sk": query, "ps": limit}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Map to NADA's result -> rows structure
            result_block = data.get('result', {})
            datasets = result_block.get('rows', [])
            
            print(f"DEBUG [Search]: Found {len(datasets)} items for query '{query}'")
            return datasets
        except Exception as e:
            print(f"ERROR [Search]: {e}")
            return []

    def get_full_metadata(self, idno):
        """
        Fetches the complete study object.
        CRITICAL: We return the full response so resources/files are preserved.
        """
        url = f"{self.base_url}/catalog/{idno}"
        print(f"DEBUG [Metadata]: Fetching details for IDNO: {idno}")
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Return the WHOLE JSON (contains 'dataset', 'resources', and 'files')
            return response.json()
        except Exception as e:
            print(f"ERROR [Metadata]: {e}")
            return {}

    def download_file(self, resource_id, save_path):
        """Downloads a resource file via the NADA access endpoint."""
        url = f"{self.base_url}/catalog/download/{resource_id}"
        
        try:
            response = requests.get(url, headers=self.headers, stream=True)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"  └─ ✅ Saved: {save_path.name}")
            
        except Exception as e:
            print(f"  └─ ❌ Download failed: {e}")
            raise e

    def parse_metadata(self, search_item, raw_data, query_string):
        """
        Maps IHSN (NADA) JSON to our SQLite Schema format.
        Aggressively hunts for files in dataset, resources, and files keys.
        """
        # NADA Structure Check
        dataset_data = raw_data.get('dataset', {})
        idno = search_item.get("idno")
        internal_id = search_item.get("id") or dataset_data.get("id")
        
        # Metadata Navigation
        metadata_block = dataset_data.get('metadata', {})
        survey_desc = metadata_block.get('survey_description', {})
        citation = survey_desc.get('citation', {}) if survey_desc else metadata_block.get('citation', {})

        project_info = {
            "query_string": query_string,
            "repository_url": "https://catalog.ihsn.org",
            "project_url": f"https://catalog.ihsn.org/index.php/catalog/{internal_id}",
            "version": citation.get('version', '1.0') if citation else '1.0',
            "title": search_item.get("title") or citation.get("title") or "Unknown Title",
            "description": search_item.get("abstract") or "No description provided.",
            "language": "en",
            "doi": citation.get('doi') if citation else None,
            "upload_date": search_item.get("changed"),
            "download_repository_folder": "ihsn",
            "download_project_folder": str(idno).replace("/", "_").replace("\\", "_") if idno else f"study_{internal_id}",
            "download_version_folder": "v1",
            "download_method": "API-IHSN"
        }

        # GREEDY FILE EXTRACTION
        # We check the root-level 'resources' and 'files' keys (where documentation lives)
        files = []
        seen_ids = set()
        
        # Sources where NADA hides file info
        potential_sources = [
            raw_data.get('resources', []),  # This usually has the PDFs/Related Materials
            raw_data.get('files', []),      # This has the data files
            dataset_data.get('resources', []),
            dataset_data.get('data_files', []),
            metadata_block.get('resources', [])
        ]

        for source in potential_sources:
            if not isinstance(source, list):
                continue
            for res in source:
                res_id = res.get('resource_id') or res.get('file_id') or res.get('id')
                if not res_id or res_id in seen_ids:
                    continue
                
                fname = res.get('filename') or res.get('file_name') or res.get('title')
                
                if fname:
                    files.append({
                        "id": res_id,
                        "name": fname,
                        "type": fname.split('.')[-1].lower() if '.' in fname else 'data'
                    })
                    seen_ids.add(res_id)

        # Keywords, Authors, and License
        keywords = dataset_data.get('tags', [])
        if isinstance(keywords, list):
            keywords = [tag.get('tag') if isinstance(tag, dict) else tag for tag in keywords]

        authors = citation.get('authoring_entity', []) if citation else []
        people = [{"name": a.get('name') if isinstance(a, dict) else a, "role": "Author"} for a in authors] if authors else []

        access_policy = dataset_data.get('data_access', {}).get('type', 'Unknown')
        license = [access_policy]

        return project_info, files, keywords, people, license