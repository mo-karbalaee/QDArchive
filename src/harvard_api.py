import requests

class HarvardDataverse:
    def __init__(self, base_url, api_key):
        """
        Initializes the Harvard Dataverse API client.
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {"X-Dataverse-key": api_key}

    def search_datasets(self, query, limit=5):
        """Searches for datasets in Dataverse."""
        url = f"{self.base_url}/api/search"
        params = {"q": query, "type": "dataset", "per_page": limit}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json().get('data', {}).get('items', [])

    def get_full_metadata(self, global_id):
        """Fetches metadata for a specific dataset using its persistent identifier."""
        url = f"{self.base_url}/api/datasets/:persistentId/?persistentId={global_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json().get('data', {}).get('latestVersion', {})

    def download_file(self, file_id, save_path):
        """Downloads the bytes of a datafile."""
        url = f"{self.base_url}/api/access/datafile/{file_id}"
        response = requests.get(url, headers=self.headers, stream=True)
        response.raise_for_status() 
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=16384):
                if chunk:
                    f.write(chunk)

    def parse_metadata(self, search_item, version_data, query_string):
        """Maps Dataverse JSON to the SQLite Schema format."""
        
        # 1. Extract values from Dataverse metadata blocks
        fields = version_data.get('metadataBlocks', {}).get('citation', {}).get('fields', [])
        
        def get_val(type_name, multiple=False):
            match = next((f for f in fields if f['typeName'] == type_name), None)
            if not match: return [] if multiple else None
            return match['value']

        raw_doi = search_item.get("global_id")
        
        # Construct the actual public landing page URL using the persistentId pattern
        # This results in: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VRSCQD
        public_project_url = f"{self.base_url}/dataset.xhtml?persistentId={raw_doi}"

        project_info = {
            "query_string": query_string,
            "repository_id": 18,  # Hardcoded repository ID
            "repository_url": self.base_url,
            "project_url": public_project_url, 
            "version": f"{version_data.get('versionNumber')}.{version_data.get('versionMinorNumber')}",
            "title": search_item.get("name"),
            "description": search_item.get("description") or "No description provided.",
            "language": get_val("language", multiple=True)[0] if get_val("language", multiple=True) else None,
            "doi": f"https://doi.org/{raw_doi.replace('doi:', '')}" if raw_doi else None,
            "upload_date": version_data.get("releaseTime", "").split("T")[0] or search_item.get("published_at", "").split("T")[0],
            "download_repository_folder": "harvard",
            "download_project_folder": raw_doi.split("/")[-1] if raw_doi else "unknown_project",
            "download_version_folder": f"v{version_data.get('versionNumber')}",
            "download_method": "API-CALL"
        }

        # 2. Keywords
        kw_entries = get_val("keyword", multiple=True)
        keywords = [k.get('keywordValue', {}).get('value') for k in kw_entries if isinstance(k, dict)]

        # 3. People/Roles
        author_entries = get_val("author", multiple=True)
        people = [{"name": a.get('authorName', {}).get('value'), "role": "Author"} for a in author_entries]

        # 4. Files
        files = []
        for f in version_data.get('files', []):
            label = f.get('label', 'unknown')
            f_id = f.get('dataFile', {}).get('id') 
            
            files.append({
                "id": f_id,
                "name": label,
                "type": label.split('.')[-1].lower() if '.' in label else 'unknown',
                "status": "SUCCEEDED" # Required for schema integrity
            })

        # 5. Licenses
        license_name = version_data.get('license', {}).get('name', 'None')
        license = [license_name]

        return project_info, files, keywords, people, license