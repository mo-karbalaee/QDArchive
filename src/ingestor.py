import os
from pathlib import Path
from database import DatabaseManager
from harvard_api import HarvardDataverse

class DataverseIngestor:
    def __init__(self, db: DatabaseManager, api: HarvardDataverse, data_root: str = "data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)

    def start(self, query: str, limit: int = 5):
        print(f"🚀 Starting Full Ingestion (Metadata + Files) for: '{query}'")
        
        items = self.api.search_datasets(query, limit)
        
        for item in items:
            project_url = item.get("url")

            # 1. Duplicate Check
            if self.db.project_exists(project_url):
                print(f"⏩ Skipping: {project_url} (Already Indexed)")
                continue

            try:
                # 2. Fetch Deep Metadata
                raw_id = item.get("global_id")
                metadata = self.api.get_full_metadata(raw_id)
                
                # 3. Parse into Schema format
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, metadata, query
                )

                # 4. Directory Management
                # Creates path like: data/harvard/DVN_ABC123/v1/
                storage_path = self.data_root / project_info['download_repository_folder'] / \
                               project_info['download_project_folder'] / \
                               project_info['download_version_folder']
                
                storage_path.mkdir(parents=True, exist_ok=True)

                # 5. File Download Loop
                print(f"📦 Dataset: {project_info['title']} ({len(files)} files)")
                
                successful_files = []
                for f in files:
                    file_save_path = storage_path / f['name']
                    
                    try:
                        print(f"  └─ Downloading: {f['name']}...", end="", flush=True)
                        self.api.download_file(f['id'], file_save_path)
                        successful_files.append(f)
                        print(" Done.")
                    except Exception as download_error:
                        print(f" Failed! (Check if restricted)")
                        # Optional: delete partially downloaded file if it exists
                        if file_save_path.exists():
                            file_save_path.unlink()

                # 6. Save to Database
                # We save to DB only after attempting the downloads
                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                print(f"✅ Transaction Complete: {project_info['title']}\n")

            except Exception as e:
                print(f"❌ Error processing {project_url}: {e}")