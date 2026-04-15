import os
from pathlib import Path

class UniversalIngestor:
    def __init__(self, db, api, data_root="data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)

    def start(self, query, limit=5):
        # Initial Query Log
        print(f"🔍 Searching for: '{query}' (limit: {limit})")
        items = self.api.search_datasets(query, limit)
        total_found = len(items)
        print(f"📊 Found {total_found} projects to process.\n")
        
        for index, item in enumerate(items, 1):
            lookup_id = item.get("idno") or item.get("global_id")
            progress = f"[{index}/{total_found}]"
            
            if self.db.project_exists(lookup_id):
                print(f"{progress} ⏩ Already indexed: {lookup_id}")
                continue

            print(f"{progress} 🚀 Processing: {lookup_id}")

            try:
                raw_data = self.api.get_full_metadata(lookup_id)
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, raw_data, query
                )

                download_count = 0
                
                if files:
                    storage_path = self.data_root / project_info['download_repository_folder'] / \
                                   project_info['download_project_folder'] / \
                                   project_info['download_version_folder']
                    
                    storage_path.mkdir(parents=True, exist_ok=True)

                    for f in files:
                        clean_name = "".join([c for c in f['name'] if c.isalnum() or c in "._- "]).strip()
                        file_save_path = storage_path / clean_name
                        
                        try:
                            self.api.download_file(f['id'], file_save_path)
                            download_count += 1
                        except Exception:
                            pass

                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                print(f"      ✅ Success: {download_count} files saved.")

            except Exception as e:
                if "UNIQUE constraint" in str(e):
                    print(f"      ⏩ Skipping: {lookup_id} (Unique Constraint)")
                else:
                    print(f"      ❌ Failed: {lookup_id} - Error: {e}")