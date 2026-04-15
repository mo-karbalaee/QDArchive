import requests
import json

class IhsnApi:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip('/')
        self.headers = {"X-API-Key": api_key}
        print(f"DEBUG [Init]: Base URL set to {self.base_url}")
        print(f"DEBUG [Init]: API Key loaded (starts with: {api_key[:4]}...)")

    def search_datasets(self, query, limit=5):
        url = f"{self.base_url}/catalog/search"
        params = {"sk": query, "ps": limit}
        
        print(f"\n--- API SEARCH START ---")
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # NADA API returns results inside 'result', which contains 'rows'
            result_block = data.get('result', {})
            
            # If 'rows' exists, that's our list of datasets
            datasets = result_block.get('rows', [])
            
            # The total is usually inside the result block too
            total_found = result_block.get('total', 0)
            
            print(f"DEBUG [Search]: HTTP 200 - Structure Recognized.")
            print(f"DEBUG [Search]: Total items in catalog: {total_found}")
            print(f"DEBUG [Search]: Items grabbed in this page: {len(datasets)}")
            
            if len(datasets) > 0:
                print(f"DEBUG [Search]: Sample IDNO from first result: {datasets[0].get('idno')}")
            
            print(f"--- API SEARCH END ---\n")
            
            return datasets

        except Exception as e:
            print(f"ERROR [Search]: {e}")
            return []

        except Exception as e:
            print(f"ERROR [Search]: Failed to execute search. {e}")
            if 'response' in locals():
                print(f"ERROR [Search]: Raw Response Text: {response.text[:500]}")
            return []

    def get_full_metadata(self, idno):
        url = f"{self.base_url}/catalog/{idno}"
        print(f"DEBUG [Metadata]: Fetching details for IDNO: {idno}")
        print(f"DEBUG [Metadata]: URL: {url}")
        
        try:
            response = requests.get(url, headers=self.headers)
            print(f"DEBUG [Metadata]: Status: {response.status_code}")
            response.raise_for_status()
            
            data = response.json()
            dataset = data.get('dataset', {})
            
            if not dataset:
                print(f"WARNING [Metadata]: 'dataset' key is missing or empty in response.")
                print(f"DEBUG [Metadata]: Available keys: {list(data.keys())}")
                
            return dataset
        except Exception as e:
            print(f"ERROR [Metadata]: {e}")
            return {}

    def download_file(self, resource_id, save_path):
        url = f"{self.base_url}/catalog/download/{resource_id}"
        print(f"DEBUG [Download]: Attempting download from {url}")
        
        try:
            response = requests.get(url, headers=self.headers, stream=True)
            print(f"DEBUG [Download]: HTTP Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"ERROR [Download]: Server returned {response.status_code}. Content: {response.text[:200]}")
            
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"DEBUG [Download]: Successfully saved to {save_path}")
            
        except Exception as e:
            print(f"ERROR [Download]: Download failed. {e}")
            raise e

    def parse_metadata(self, search_item, dataset_data, query_string):
        """
        Maps IHSN (NADA) JSON to our SQLite Schema format.
        Uses numeric 'id' for URLs and string 'idno' for file storage.
        """
        # 1. Extract IDs
        # idno is the string (e.g., ZAF_1985_BMRS_v01_M)
        # internal_id is the number (e.g., 1022) required for valid URLs
        idno = search_item.get("idno")
        internal_id = search_item.get("id") or dataset_data.get("id")
        
        # 2. Navigate Metadata Blocks
        metadata = dataset_data.get('metadata', {})
        
        # NADA often nests survey info under 'survey_description' -> 'citation'
        survey_desc = metadata.get('survey_description', {})
        citation = survey_desc.get('citation', {}) if survey_desc else metadata.get('citation', {})

        # 3. Format Project Info
        project_info = {
            "query_string": query_string,
            "repository_url": "https://catalog.ihsn.org",
            # FIX: The web catalog strictly requires the numeric ID for the URL path
            "project_url": f"https://catalog.ihsn.org/index.php/catalog/{internal_id}",
            "version": citation.get('version', '1.0') if citation else '1.0',
            "title": search_item.get("title", "Unknown Title"),
            "description": search_item.get("abstract") or "No description provided.",
            "language": "en",
            "doi": citation.get('doi') if citation else None,
            "upload_date": search_item.get("changed"),
            "download_repository_folder": "ihsn",
            # KEEP: Using the IDNO string for folders makes them much easier to identify
            "download_project_folder": str(idno).replace("/", "_").replace("\\", "_") if idno else f"study_{internal_id}",
            "download_version_folder": "v1",
            "download_method": "API-IHSN"
        }

        # 4. Extract Resources (Files)
        files = []
        resources = dataset_data.get('resources', [])
        for res in resources:
            res_id = res.get('resource_id') or res.get('id')
            fname = res.get('filename') or res.get('title') or f"file_{res_id}"
            
            files.append({
                "id": res_id,
                "name": fname,
                "type": fname.split('.')[-1] if '.' in fname else 'data'
            })

        # 5. Extract Keywords
        keywords = dataset_data.get('tags', [])
        if isinstance(keywords, list):
            keywords = [tag.get('tag') if isinstance(tag, dict) else tag for tag in keywords]

        # 6. Extract Authors/People
        authors = citation.get('authoring_entity', []) if citation else []
        people = []
        if isinstance(authors, list):
            for a in authors:
                name = a.get('name') if isinstance(a, dict) else a
                if name:
                    people.append({"name": name, "role": "Author"})

        # 7. Extract License/Access Policy
        access_policy = dataset_data.get('data_access', {}).get('type', 'Unknown')
        license = [access_policy]

        return project_info, files, keywords, people, license