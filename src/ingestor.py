import os
from pathlib import Path
from database import DatabaseManager

class UniversalIngestor:
    def __init__(self, db: DatabaseManager, api, data_root: str = "data"):
        self.db = db
        self.api = api
        self.data_root = Path(data_root)

    def start(self, query: str, limit: int = 5):
        repo_name = self.api.__class__.__name__
        print(f"🚀 Starting Ingestion via {repo_name} for: '{query}'")
        
        items = self.api.search_datasets(query, limit)
        
        for item in items:
            # --- THE SKIP FIX ---
            # We identify the project by its unique ID (global_id for Harvard, idno for IHSN)
            # This is much safer than checking the URL string.
            lookup_id = item.get("global_id") or item.get("idno")
            
            if not lookup_id:
                continue

            # Check the DB using the unique identifier
            # (Make sure your DatabaseManager has a method or logic for this)
            if self.db.project_exists(lookup_id):
                print(f"⏩ Skipping: {lookup_id} (Already Indexed)")
                continue

            try:
                # Fetch full metadata (now returns root object for IHSN)
                metadata = self.api.get_full_metadata(lookup_id)
                
                # Parse
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, metadata, query
                )

                # Directory setup
                storage_path = self.data_root / project_info['download_repository_folder'] / \
                               project_info['download_project_folder'] / \
                               project_info['download_version_folder']
                storage_path.mkdir(parents=True, exist_ok=True)

                # Download Loop
                print(f"📦 Dataset: {project_info['title']} ({len(files)} files)")
                
                for f in files:
                    clean_filename = "".join([c for c in f['name'] if c.isalnum() or c in "._- "]).strip()
                    file_save_path = storage_path / clean_filename
                    
                    try:
                        self.api.download_file(f['id'], file_save_path)
                    except Exception as download_error:
                        print(f"  └─ ❌ Failed {clean_filename}: {download_error}")
                        if file_save_path.exists():
                            file_save_path.unlink()

                # Save to Database
                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                print(f"✅ Indexed: {project_info['title']}\n")

            except Exception as e:
                # Catching the UNIQUE constraint error here if the ID check failed
                if "UNIQUE constraint failed" in str(e):
                    print(f"⏩ Skipping {lookup_id}: Already in database (Constraint Triggered)")
                else:
                    print(f"❌ Error processing {lookup_id}: {e}")