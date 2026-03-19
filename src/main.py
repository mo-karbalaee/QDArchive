import os
import requests
from dotenv import load_dotenv

# 1. Load the variables from .env into the environment
# uv run will often do this automatically, but keeping this 
# line ensures the script works in any environment.
load_dotenv()

# 2. Retrieve the variables from your .env file
# Ensure these keys match exactly what is written in your .env file
api_key = os.getenv("HARVARD_API_TOKEN")
base_url = os.getenv("HARVARD_BASE_URL")

# Safety check to prevent cryptic "401 Unauthorized" errors later
if not api_key:
    raise ValueError("API Key 'HARVARD_API_TOKEN' not found! Check your .env file.")
if not base_url:
    print("Warning: HARVARD_BASE_URL not found. Defaulting to Harvard Production.")
    base_url = "https://dataverse.harvard.edu"

# 3. Setup Headers for Dataverse Authentication
headers = {
    "X-Dataverse-key": api_key
}

def search_test(query="climate"):
    """Simple test function to verify the connection."""
    endpoint = f"{base_url}/api/search"
    params = {"q": query}
    
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raises an error for 4xx/5xx responses
        
        data = response.json()
        total_found = data.get('data', {}).get('total_count', 0)
        
        print(f"✅ Success! Status Code: {response.status_code}")
        print(f"🔍 Search for '{query}' found {total_found} results.")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection Error: {e}")

if __name__ == "__main__":
    search_test()