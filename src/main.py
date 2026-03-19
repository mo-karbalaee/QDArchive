import os
import requests
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load variables
load_dotenv()
API_KEY = os.getenv("HARVARD_API_TOKEN")
BASE_URL = os.getenv("HARVARD_BASE_URL", "https://dataverse.harvard.edu")
DB_PATH = "metadata.db"
DATA_ROOT = Path("data/harvard")

headers = {"X-Dataverse-key": API_KEY}

def format_doi(raw_doi):
    """Ensures DOI is a full URL with https://doi.org/ prefix."""
    if not raw_doi:
        return None
    # Remove 'doi:' prefix if present and prepend the URL
    clean_doi = raw_doi.replace("doi:", "")
    return f"https://doi.org/{clean_doi}"

def project_exists(cursor, doi_url):
    """Checks if the DOI already exists in the PROJECTS table."""
    cursor.execute('SELECT id FROM PROJECTS WHERE doi = ?', (doi_url,))
    return cursor.fetchone() is not None

def save_to_db(cursor, project_data, files, keywords, people, licenses):
    # 1. Insert Project into PROJECTS table
    cursor.execute('''
        INSERT INTO PROJECTS (
            query_string, repository_id, repository_url, project_url, 
            version, title, description, doi, download_method,
            download_repository_folder, download_project_folder, download_version_folder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        project_data['query'], 1, BASE_URL, project_data['url'],
        project_data['version'], project_data['title'], project_data['description'],
        project_data['doi'], "API-CALL", "harvard", 
        project_data['project_folder'], project_data['version_folder']
    ))
    project_id = cursor.lastrowid

    # 2. Insert Files
    for f in files:
        cursor.execute('INSERT INTO FILES (project_id, file_name, file_type) VALUES (?, ?, ?)',
                       (project_id, f['name'], f['type']))

    # 3. Insert Keywords
    for kw in keywords:
        cursor.execute('INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)', (project_id, kw))

    # 4. Insert People
    for p in people:
        cursor.execute('INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)',
                       (project_id, p['name'], p['role']))

    # 5. Insert Licenses
    for l in licenses:
        cursor.execute('INSERT INTO LICENSES (project_id, license) VALUES (?, ?)', (project_id, l))

    print(f"✅ Saved Project: {project_data['title']} (ID: {project_id})")
    return project_id

def fetch_and_store(query="climate", limit=3):
    search_url = f"{BASE_URL}/api/search"
    params = {"q": query, "type": "dataset", "per_page": limit}
    
    res = requests.get(search_url, headers=headers, params=params)
    items = res.json().get('data', {}).get('items', [])

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for item in items:
        # 1. Format DOI and check for duplicates
        raw_doi = item.get("global_id")
        doi_url = format_doi(raw_doi)
        
        if project_exists(cursor, doi_url):
            print(f"⏩ Skipping {doi_url} (Already in database)")
            continue

        # 2. Get Deep Metadata via Native API
        meta_url = f"{BASE_URL}/api/datasets/:persistentId/?persistentId={raw_doi}"
        meta_res = requests.get(meta_url, headers=headers).json()
        
        ds_version = meta_res.get('data', {}).get('latestVersion', {})
        version_str = f"v{ds_version.get('versionNumber', '1')}"
        
        # 3. Extract Metadata Fields
        metadata_blocks = ds_version.get('metadataBlocks', {}).get('citation', {}).get('fields', [])
        get_val = lambda name: next((f['value'] for f in metadata_blocks if f['typeName'] == name), None)

        # Folder Logic: use the unique part of the DOI as the project folder
        safe_doi_name = raw_doi.split("/")[-1] 
        
        project_data = {
            "query": query,
            "url": item.get("url"),
            "title": item.get("name"),
            "description": item.get("description"),
            "doi": doi_url,
            "version": version_str,
            "project_folder": safe_doi_name,
            "version_folder": version_str
        }

        # 4. Extract Keywords, People, Licenses
        kw_list = get_val("keyword") or []
        keywords = [k.get('keywordValue', {}).get('value') for k in kw_list if isinstance(k, dict)]

        author_list = get_val("author") or []
        people = [{"name": a.get('authorName', {}).get('value'), "role": "Author"} for a in author_list]

        license = [ds_version.get('license', {}).get('name', 'Unknown')]

        # 5. Extract Files info
        files = []
        for f_meta in ds_version.get('files', []):
            f_name = f_meta.get('label', 'unknown_file')
            f_ext = f_name.split('.')[-1] if '.' in f_name else 'unknown'
            files.append({"name": f_name, "type": f_ext})

        # 6. Save everything to DB
        save_to_db(cursor, project_data, files, keywords, people, license)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    fetch_and_store("climate")