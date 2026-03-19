import os
from dotenv import load_dotenv
from database import DatabaseManager
from harvard_api import HarvardDataverse

def run_ingestion(query, limit=5):
    # 1. Setup
    load_dotenv()
    db = DatabaseManager(os.getenv("DB_PATH", "metadata.db"))
    api = HarvardDataverse(
        os.getenv("HARVARD_BASE_URL", "https://dataverse.harvard.edu"), 
        os.getenv("HARVARD_API_TOKEN")
    )

    print(f"🚀 Starting ingestion for query: '{query}'")

    # 2. Search for datasets
    items = api.search_datasets(query, limit)
    print(f"🔎 Found {len(items)} datasets. Processing...")

    for item in items:
        raw_doi = item.get("global_id")
        # Ensure we check for the formatted DOI URL
        doi_url = f"https://doi.org/{raw_doi.replace('doi:', '')}"

        # 3. Duplicate Check
        if db.project_exists(doi_url):
            print(f"⏩ Skipping {doi_url} (Already in Database)")
            continue

        try:
            # 4. Fetch Deep Metadata
            metadata = api.get_full_metadata(raw_doi)
            
            # 5. Parse into Schema format
            project_info, files, keywords, people, licenses = api.parse_metadata(
                item, metadata, query
            )

            # 6. Save to Database (Uncommented and Functional!)
            db.insert_project_data(project_info, files, keywords, people, licenses)
            
            print(f"✅ Successfully saved: {project_info['title']}")

        except Exception as e:
            print(f"❌ Failed to process {raw_doi}: {e}")

if __name__ == "__main__":
    # You can change the query and limit here
    run_ingestion("climate change", limit=10)