import os
from dotenv import load_dotenv
from database import DatabaseManager
from harvard_api import HarvardDataverse
from ingestor import DataverseIngestor

def main():
    # 1. Load configuration
    load_dotenv()
    
    # 2. Initialize low-level components (Infrastructure)
    db = DatabaseManager(os.getenv("DB_PATH", "metadata.db"))
    api = HarvardDataverse(
        os.getenv("HARVARD_BASE_URL"),
        os.getenv("HARVARD_API_TOKEN")
    )

    # 3. Initialize high-level service (Logic)
    ingestor = DataverseIngestor(db, api)

    # 4. Run the process
    ingestor.start(query="FAU", limit=10)

if __name__ == "__main__":
    main()