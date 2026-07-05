import os
import requests
from dotenv import load_dotenv
from pathlib import Path

def test_api_download_capability():
    # 1. Load Environment
    load_dotenv()
    api_key = os.getenv("HARVARD_API_TOKEN")
    base_url = os.getenv("HARVARD_BASE_URL", "https://dataverse.harvard.edu").rstrip('/')
    
    if not api_key:
        print("❌ ERROR: HARVARD_API_TOKEN not found in .env file.")
        return

    headers = {"X-Dataverse-key": api_key}
    
    # Ensure the tests folder exists
    tests_dir = Path("tests")
    tests_dir.mkdir(exist_ok=True)
    
    print(f"--- Starting Download Test ---")
    print(f"Targeting: {base_url}")

    try:
        # 2. STEP ONE: Find a public file to test with
        search_endpoint = f"{base_url}/api/search"
        search_params = {
            "q": "interview",
            "type": "file",
            "per_page": 1
        }
        
        print("🔍 Searching for a test file...")
        search_res = requests.get(search_endpoint, headers=headers, params=search_params)
        search_res.raise_for_status()
        
        items = search_res.json().get('data', {}).get('items', [])
        if not items:
            print("❓ No public files found. Try a different query.")
            return

        test_file = items[0]
        file_id = test_file.get('file_id')
        file_name = test_file.get('name', f"file_{file_id}.bin")
        print(f"✅ Found File: '{file_name}' (ID: {file_id})")

        # 3. STEP TWO: Attempt to download the file
        download_url = f"{base_url}/api/access/datafile/{file_id}"
        print(f"📡 Downloading to {tests_dir}/...")
        
        # Save path inside the tests folder
        save_path = tests_dir / file_name

        with requests.get(download_url, headers=headers, stream=True) as r:
            if r.status_code == 200:
                with open(save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                print(f"✨ SUCCESS! File saved to: {save_path}")
                print(f"📏 Size: {os.path.getsize(save_path)} bytes")
                
            elif r.status_code == 401:
                print("❌ FAILED: Status 401. Your API Key is invalid.")
            elif r.status_code == 403:
                print("❌ FAILED: Status 403. This file is restricted.")
            else:
                print(f"❓ FAILED: Status code {r.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"💥 CONNECTION ERROR: {e}")

if __name__ == "__main__":
    test_api_download_capability()