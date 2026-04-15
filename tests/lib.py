import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
PROJECT_ID = "7124" 
BASE_URL = "https://catalog.ihsn.org"
SAVE_DIR = "downloads"
# ==========================================

def scrape_and_download(project_id):
    # We still need the API Key if the site requires a session, 
    # but usually 'Related Materials' are publicly visible.
    api_token = os.getenv("IHSN_API_TOKEN")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-API-KEY": api_token
    }

    # 1. Scrape the Related Materials page
    target_url = f"{BASE_URL}/catalog/{project_id}/related-materials"
    print(f"🔍 Scraping materials from: {target_url}")
    
    response = requests.get(target_url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Failed to load page: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 2. Find all download links
    # These typically look like /catalog/7124/download/85641
    download_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if f"/catalog/{project_id}/download/" in href:
            # Ensure we have a full URL
            full_url = href if href.startswith('http') else f"{BASE_URL}{href}"
            
            # Try to get the filename from the UI text or title attribute
            title = a.get('title') or a.text.strip()
            # Clean up the title to use as a filename
            clean_name = "".join([c for c in title if c.isalnum() or c in "._- "]).strip()
            if not clean_name:
                clean_name = href.split('/')[-1]
            
            download_links.append({"url": full_url, "name": clean_name})

    if not download_links:
        print("⚠️ No download links found on the page.")
        return

    print(f"📦 Found {len(download_links)} files. Starting download...\n")

    # 3. Create the folder
    output_path = Path(SAVE_DIR) / project_id
    output_path.mkdir(parents=True, exist_ok=True)

    # 4. Download Loop
    for link in download_links:
        file_url = link['url']
        file_name = link['name']
        
        # Ensure it has a PDF extension if it looks like a document
        if "." not in file_name:
            file_name += ".pdf"

        save_to = output_path / file_name
        print(f"📥 Downloading: {file_name}...", end="", flush=True)

        try:
            with requests.get(file_url, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(save_to, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=16384):
                        f.write(chunk)
            print(" Done.")
        except Exception as e:
            print(f" FAILED: {e}")

    print(f"\n✅ Finished. Files saved in: {output_path.absolute()}")

if __name__ == "__main__":
    scrape_and_download(PROJECT_ID)