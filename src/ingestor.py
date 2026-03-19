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
        print(f"🚀 Starting ingestion for: '{query}'")
        items = self.api.search_datasets(query, limit)
        
        for item in items:
            project_url = item.get("url")

            if self.db.project_exists(project_url):
                print(f"⏩ Skipping: {project_url}")
                continue

            try:
                # 1. Fetch & Parse
                raw_id = item.get("global_id")
                metadata = self.api.get_full_metadata(raw_id)
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, metadata, query
                )

                # 2. Handle Directories
                storage_path = self.data_root / project_info['download_repository_folder'] / \
                               project_info['download_project_folder'] / \
                               project_info['download_version_folder']
                storage_path.mkdir(parents=True, exist_ok=True)

                # 3. Download Files
                print(f"📦 Dataset: {project_info['title']} ({len(files)} files)")
                for f in files:
                    file_save_path = storage_path / f['name']
                    self.api.download_file(f['id'], file_save_path)
                
                # 4. Save to Database
                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                print(f"✅ Saved to DB\n")

            except Exception as e:
                print(f"❌ Error processing {project_url}: {e}")