import os
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load IHSN_API_TOKEN from your .env file
load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
# Use the IDNO (String) for this endpoint as per your discovery
DATASET_IDNO = "4179" 
BASE_URL = "https://catalog.ihsn.org/index.php/api"
SAVE_DIR = "downloads"
# ==========================================

def download_from_ihsn():
    api_token = os.getenv("IHSN_API_TOKEN")
    if not api_token:
        print("❌ ERROR: IHSN_API_TOKEN not found in .env")
        return

    headers = {"X-API-Key": api_token}
    
    # THE ENDPOINT YOU FOUND:
    # http://ihsn.github.io/index.php/api/datasets/{datasetIDNo}/resources
    resources_url = f"{BASE_URL}/catalog/{DATASET_IDNO}/resources"
    
    print(f"🚀 Targeted Download for IDNO: {DATASET_IDNO}")
    print(f"🔍 Fetching resource list...")

    try:
        response = requests.get(resources_url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # NADA returns resources in a 'resources' array
        resource_list = data.get('resources', [])
        
        if not resource_list:
            print(f"⚠️ No resources found for {DATASET_IDNO}. JSON Response: {data}")
            return

        print(f"📦 Found {len(resource_list)} resources. Starting downloads...\n")

        # Create folder only if we have files
        output_path = Path(SAVE_DIR) / DATASET_IDNO
        output_path.mkdir(parents=True, exist_ok=True)

        for res in resource_list:
            # We need the numeric resource ID for the download endpoint
            resource_id = res.get('resource_id') or res.get('id')
            filename = res.get('filename') or res.get('title') or f"file_{resource_id}"
            
            # Sanitize filename
            clean_name = "".join([c for c in filename if c.isalnum() or c in "._- "]).strip()
            if "." not in clean_name:
                clean_name += ".pdf"

            download_url = f"{BASE_URL}/catalog/download/{resource_id}"
            save_to = output_path / clean_name

            print(f"📥 Downloading: {clean_name}...", end="", flush=True)
            
            try:
                with requests.get(download_url, headers=headers, stream=True, timeout=60) as dl:
                    dl.raise_for_status()
                    with open(save_to, 'wb') as f:
                        for chunk in dl.iter_content(chunk_size=16384):
                            f.write(chunk)
                print(" Done.")
            except Exception as e:
                print(f" FAILED: {e}")

    except Exception as e:
        print(f"❌ Error accessing the resource list: {e}")

    print(f"\n✅ Finished. Files are in: {output_path.absolute()}")

if __name__ == "__main__":
    download_from_ihsn()