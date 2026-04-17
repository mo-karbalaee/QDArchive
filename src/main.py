import os
from dotenv import load_dotenv
from database import DatabaseManager
from harvard_api import HarvardDataverse
from ihsn_api import IhsnApi
from ingestor import UniversalIngestor
from models.query_terms import qualitative_queries
import time

def main():
    # 1. Load configuration
    load_dotenv()
    
    # 2. Initialize the shared Database
    db = DatabaseManager()
    
    # 3. Define the query parameters
    SEARCH_QUERY = "love"
    LIMIT_PER_QUERY = 1

    # 4. Define the repositories to process sequentially
    repositories = [
        {
            "name": "Harvard Dataverse & Harvard Murray",
            "icon": "🏛️",
            "class": HarvardDataverse,
            "base_url": os.getenv("HARVARD_BASE_URL"),
            "api_token": os.getenv("HARVARD_API_TOKEN")
        },
        {
            "name": "IHSN Catalog",
            "icon": "🌏",
            "class": IhsnApi,
            "base_url": os.getenv("IHSN_BASE_URL"),
            "api_token": os.getenv("IHSN_API_TOKEN")
        }
    ]

    for query in qualitative_queries:
        print(f"\n{'#'*60}")
        print(f"🔎 GLOBAL QUERY: '{query}'")
        print(f"{'#'*60}")

        for repo in repositories:
            if not repo["base_url"] or not repo["api_token"]:
                continue

            print(f"\n{repo['icon']} Repository: {repo['name']}")

            try:
                api = repo["class"](repo["base_url"], repo["api_token"])
                ingestor = UniversalIngestor(db, api, data_root="data")

                ingestor.start(query=query, limit=LIMIT_PER_QUERY)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Error in {repo['name']} for query '{query}': {e}")

    print(f"\n✅ All {len(qualitative_queries)} queries processed.")

if __name__ == "__main__":
    main()