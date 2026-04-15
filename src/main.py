import os
from dotenv import load_dotenv
from database import DatabaseManager
from harvard_api import HarvardDataverse
from ihsn_api import IhsnApi
from ingestor import UniversalIngestor

def main():
    # 1. Load configuration
    load_dotenv()
    
    # 2. Initialize the shared Database
    db = DatabaseManager()
    
    # 3. Define the query parameters
    SEARCH_QUERY = "qualitative research"
    LIMIT_PER_REPO = 3

    # 4. Define the repositories to process sequentially
    repositories = [
        {
            "name": "Harvard Dataverse",
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

    # 5. Execute sequential ingestion
    for repo in repositories:
        # Check if environment variables are set for this repo
        if not repo["base_url"] or not repo["api_token"]:
            print(f"⚠️  Skipping {repo['name']}: Missing URL or Token in .env")
            continue

        print(f"\n{'='*40}")
        print(f"{repo['icon']}  Targeting: {repo['name']}")
        print(f"{'='*40}")

        try:
            # Initialize the specific API strategy
            api = repo["class"](repo["base_url"], repo["api_token"])
            
            # Initialize the Ingestor with the selected API
            ingestor = UniversalIngestor(db, api, data_root="data")

            # Run the ingestion process
            ingestor.start(query=SEARCH_QUERY, limit=LIMIT_PER_REPO)
            
        except Exception as e:
            print(f"❌ Critical error processing {repo['name']}: {e}")

    print(f"\n✅ All scheduled ingestions complete.")

if __name__ == "__main__":
    main()