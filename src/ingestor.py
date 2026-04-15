import os
from pathlib import Path

class UniversalIngestor:
    def __init__(self, db, api, data_root="data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)

    def start(self, query, limit=5):
        items = self.api.search_datasets(query, limit)
        
        for item in items:
            lookup_id = item.get("idno") or item.get("global_id")
            
            # Check DB by ID to avoid unique constraint crashes
            if self.db.project_exists(lookup_id):
                print(f"⏩ Skipping: {lookup_id}")
                continue

            try:
                raw_data = self.api.get_full_metadata(lookup_id)
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, raw_data, query
                )

                if not files:
                    print(f"➖ Dataset {lookup_id} has 0 files. Skipping folder creation.")
                else:
                    # ONLY CREATE FOLDER IF THERE ARE FILES
                    storage_path = self.data_root / project_info['download_repository_folder'] / \
                                   project_info['download_project_folder'] / \
                                   project_info['download_version_folder']
                    
                    print(f"📦 Dataset: {project_info['title']} ({len(files)} files)")
                    storage_path.mkdir(parents=True, exist_ok=True)

                    for f in files:
                        clean_name = "".join([c for c in f['name'] if c.isalnum() or c in "._- "]).strip()
                        file_save_path = storage_path / clean_name
                        
                        try:
                            self.api.download_file(f['id'], file_save_path)
                            print(f"  └─ ✅ Downloaded: {clean_name}")
                        except Exception as e:
                            print(f"  └─ ❌ Failed {clean_name}: {e}")

                # Save to DB regardless so we don't keep hitting the API for empty projects
                self.db.insert_project_data(project_info, files, keywords, people, licenses)

            except Exception as e:
                if "UNIQUE constraint" in str(e):
                    print(f"⏩ Already indexed: {lookup_id}")
                else:
                    print(f"❌ Error: {e}")