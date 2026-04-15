import os
from pathlib import Path
from database import DatabaseManager

class UniversalIngestor:
    def __init__(self, db: DatabaseManager, api, data_root: str = "data"):
        """
        :param db: DatabaseManager instance
        :param api: Either HarvardDataverse or IhsnApi instance
        :param data_root: Root directory for file storage
        """
        self.db = db
        self.api = api
        self.data_root = Path(data_root)

    def start(self, query: str, limit: int = 5):
        # Dynamically determine repository name for the logs
        repo_name = self.api.__class__.__name__
        print(f"🚀 Starting Ingestion via {repo_name} for: '{query}'")
        
        items = self.api.search_datasets(query, limit)
        
        for item in items:
            # Both APIs return a 'url' in the search results
            project_url = item.get("url")

            # 1. Duplicate Check
            if self.db.project_exists(project_url):
                print(f"⏩ Skipping: {project_url} (Already Indexed)")
                continue

            try:
                # 2. Fetch Deep Metadata
                # Harvard uses 'global_id', IHSN uses 'idno'
                # We try both to stay universal
                lookup_id = item.get("global_id") or item.get("idno")
                
                if not lookup_id:
                    print(f"⚠️ Could not find a valid ID for {project_url}")
                    continue

                metadata = self.api.get_full_metadata(lookup_id)
                
                # 3. Parse into Schema format
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, metadata, query
                )

                # 4. Directory Management
                storage_path = self.data_root / project_info['download_repository_folder'] / \
                               project_info['download_project_folder'] / \
                               project_info['download_version_folder']
                
                storage_path.mkdir(parents=True, exist_ok=True)

                # 5. File Download Loop
                print(f"📦 Dataset: {project_info['title']} ({len(files)} files)")
                
                for f in files:
                    # Sanitize filename (remove characters that cause OS errors)
                    clean_filename = "".join([c for c in f['name'] if c.isalnum() or c in "._- "]).strip()
                    file_save_path = storage_path / clean_filename
                    
                    try:
                        print(f"  └─ Downloading: {clean_filename}...", end="", flush=True)
                        # The 'id' here is the file/resource ID parsed in the API class
                        self.api.download_file(f['id'], file_save_path)
                        print(" Done.")
                    except Exception as download_error:
                        print(f" Failed! (Reason: {download_error})")
                        if file_save_path.exists():
                            file_save_path.unlink()

                # 6. Save to Database
                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                print(f"✅ Transaction Complete: {project_info['title']}\n")

            except Exception as e:
                print(f"❌ Error processing {project_url}: {e}")