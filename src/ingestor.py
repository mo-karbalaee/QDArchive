import os
from pathlib import Path

class UniversalIngestor:
    def __init__(self, db, api, data_root="data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)
        # Comprehensive list of video and audio extensions to skip
        self.ignored_types = {
            'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'm4v', 'webm', # Video
            'mp3', 'wav', 'aac', 'flac', 'ogg', 'wma', 'm4a'         # Audio
        }

    def start(self, query, limit=5):
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
                # Filter out both video and audio files based on extension
                valid_files = [f for f in files if f['type'] not in self.ignored_types]
                
                if valid_files:
                    storage_path = self.data_root / project_info['download_repository_folder'] / \
                                   project_info['download_project_folder'] / \
                                   project_info['download_version_folder']
                    
                    storage_path.mkdir(parents=True, exist_ok=True)

                    for f in valid_files:
                        file_save_path = storage_path / f['name']
                        try:
                            self.api.download_file(f['id'], file_save_path)
                            download_count += 1
                        except Exception:
                            pass

                # Save the sanitized list to the database
                self.db.insert_project_data(project_info, valid_files, keywords, people, licenses)
                print(f"      ✅ Success: {download_count} files saved.")

            except Exception as e:
                if "UNIQUE constraint" in str(e):
                    print(f"      ⏩ Already indexed: {lookup_id}")
                else:
                    print(f"      ❌ Failed: {lookup_id} - Error: {e}")