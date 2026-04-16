import os
import requests
from pathlib import Path
from models.download_result import DownloadResult 

class UniversalIngestor:
    def __init__(self, db, api, data_root="data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)
        self.ignored_types = {
            'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'm4v', 'webm', 
            'mp3', 'wav', 'aac', 'flac', 'ogg', 'wma', 'm4a'         
        }

    def start(self, query, limit=5):
        print(f"🔍 Searching for: '{query}' (limit: {limit})")
        items = self.api.search_datasets(query, limit)
        total_found = len(items)
        print(f"📊 Found {total_found} projects to process.\n")
        
        for index, item in enumerate(items, 1):
            # Keep lookup_id as it was for DB checks and processing
            lookup_id = item.get("idno") or item.get("global_id")
            progress = f"[{index}/{total_found}]"
            
            if self.db.project_exists(lookup_id):
                print(f"{progress} ⏩ Already indexed: {lookup_id}")
                continue

            print(f"{progress} 🚀 Processing: {lookup_id}")

            try:
                # --- DIFFERENTIATED CALL HERE ---
                # If the repository is IHSN (id 9), pass the numeric ID
                # Otherwise, pass the standard lookup_id
                target_id = item.get("id") if item.get("repository_id") == 9 or "ihsn" in str(self.api.__class__.__name__).lower() else lookup_id
                
                raw_data = self.api.get_full_metadata(target_id)
                
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, raw_data, query
                )

                # Filter media types
                valid_files = [f for f in files if f['type'] not in self.ignored_types]
                
                download_count = 0
                if valid_files:
                    storage_path = self.data_root / \
                                   str(project_info['download_repository_folder']) / \
                                   str(project_info['download_project_folder']) / \
                                   str(project_info['download_version_folder'])
                    
                    storage_path.mkdir(parents=True, exist_ok=True)

                    for f in valid_files:
                        if not f.get('name'):
                            f['status'] = DownloadResult.FAILED_SERVER_UNRESPONSIVE.name
                            continue

                        file_save_path = storage_path / f['name']
                        
                        try:
                            self.api.download_file(f['id'], file_save_path)
                            f['status'] = DownloadResult.SUCCEEDED.name
                            download_count += 1
                        
                        except requests.exceptions.HTTPError as e:
                            status_code = e.response.status_code
                            if status_code in [401, 403]:
                                f['status'] = DownloadResult.FAILED_LOGIN_REQUIRED.name
                            elif status_code == 413: 
                                f['status'] = DownloadResult.FAILED_TOO_LARGE.name
                            else:
                                f['status'] = DownloadResult.FAILED_SERVER_UNRESPONSIVE.name
                            print(f"      ⚠️  File skip: {f['name']} ({f['status']})")
                            
                        except Exception as e:
                            f['status'] = DownloadResult.FAILED_SERVER_UNRESPONSIVE.name
                            print(f"      ⚠️  File error: {f['name']} - {e}")

                self.db.insert_project_data(project_info, valid_files, keywords, people, licenses)
                print(f"      ✅ Success: {download_count} files saved.")

            except Exception as e:
                if "UNIQUE constraint" in str(e):
                    print(f"      ⏩ Already indexed: {lookup_id}")
                else:
                    print(f"      ❌ Failed: {lookup_id} - Error: {e}")