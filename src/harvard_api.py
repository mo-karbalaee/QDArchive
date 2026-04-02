import requests

class HarvardDataverse:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip('/')
        self.headers = {"X-Dataverse-key": api_key}

    def search_datasets(self, query, limit=5):
        url = f"{self.base_url}/api/search"
        params = {"q": query, "type": "dataset", "per_page": limit}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json().get('data', {}).get('items', [])

    def get_full_metadata(self, global_id):
        """Fetches the deep technical metadata for a dataset."""
        url = f"{self.base_url}/api/datasets/:persistentId/?persistentId={global_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json().get('data', {}).get('latestVersion', {})

    def parse_metadata(self, search_item, version_data, query_string):
        """Maps Dataverse JSON to your SQLite Schema format."""
        
        # 1. Extract Citation Fields
        fields = version_data.get('metadataBlocks', {}).get('citation', {}).get('fields', [])
        
        def get_val(type_name, multiple=False):
            match = next((f for f in fields if f['typeName'] == type_name), None)
            if not match: return [] if multiple else None
            return match['value']

        # 2. Format basic project info
        raw_doi = search_item.get("global_id")
        project_info = {
            "query_string": query_string,
            "repository_url": self.base_url,
            "project_url": search_item.get("url"),
            "version": f"v{version_data.get('versionNumber')}.{version_data.get('versionMinorNumber')}",
            "title": search_item.get("name"),
            "description": search_item.get("description"),
            "language": get_val("language", multiple=True)[0] if get_val("language", multiple=True) else "en",
            "doi": f"https://doi.org/{raw_doi.replace('doi:', '')}",
            "upload_date": version_data.get("releaseTime", "").split("T")[0],
            "download_repository_folder": "harvard",
            "download_project_folder": raw_doi.split("/")[-1],
            "download_version_folder": f"v{version_data.get('versionNumber')}",
            "download_method": "API-CALL"
        }

        # 3. Format Keywords
        kw_entries = get_val("keyword", multiple=True)
        keywords = [k.get('keywordValue', {}).get('value') for k in kw_entries if isinstance(k, dict)]

        # 4. Format People/Roles
        author_entries = get_val("author", multiple=True)
        people = [{"name": a.get('authorName', {}).get('value'), "role": "Author"} for a in author_entries]

        # 5. Format Files
        files = []
        for f in version_data.get('files', []):
            print(f)
            label = f.get('label', 'unknown')
            files.append({
                "name": label,
                "type": label.split('.')[-1] if '.' in label else 'unknown'
            })

        # 6. License
        license = [version_data.get('license', {}).get('name', 'None')]

        return project_info, files, keywords, people, license