import os
from dotenv import load_dotenv
from database import DatabaseManager
from harvard_api import HarvardDataverse
from ihsn_api import IhsnApi
from ingestor import UniversalIngestor

def main():
    # 1. Load configuration
    load_dotenv()
    
    db = DatabaseManager()
    
    # 3. Choose your Repository Strategy
    # You can change this to "harvard" or "ihsn"
    target_repo = "ihsn" 

    if target_repo == "harvard":
        print("🏛️  Targeting: Harvard Dataverse")
        api = HarvardDataverse(
            os.getenv("HARVARD_BASE_URL"),
            os.getenv("HARVARD_API_TOKEN")
        )
    elif target_repo == "ihsn":
        print("🌏 Targeting: IHSN Catalog")
        api = IhsnApi(
            os.getenv("IHSN_BASE_URL"),
            os.getenv("IHSN_API_TOKEN")
        )
    else:
        print("❌ Unknown repository specified.")
        return

    # 4. Initialize the Ingestor with the selected API
    ingestor = UniversalIngestor(db, api, data_root="data")

    # 5. Run the process
    # Note: IHSN might return different results for 'qualitative' than Harvard
    ingestor.start(query="qualitative research", limit=3)

if __name__ == "__main__":
    main()