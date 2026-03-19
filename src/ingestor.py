from pathlib import Path
from database import DatabaseManager
from harvard_api import HarvardDataverse

class DataverseIngestor:
    def __init__(self, db: DatabaseManager, api: HarvardDataverse):
        self.db = db
        self.api = api

    def start(self, query: str, limit: int = 5):
        print(f"🚀 Starting Metadata Ingestion for: '{query}'")
        print(f"⚠️  Note: File downloads are DISABLED. Only database records will be created.")
        
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
                # (Still returns 'files' list so we can index filenames in the DB)
                project_info, files, keywords, people, licenses = self.api.parse_metadata(
                    item, metadata, query
                )

                # 4. Save to Database
                # We still record the metadata about files in the FILES table
                self.db.insert_project_data(project_info, files, keywords, people, licenses)
                
                print(f"✅ Indexed: {project_info['title']} ({len(files)} file records)")

            except Exception as e:
                print(f"❌ Error processing {project_url}: {e}")

