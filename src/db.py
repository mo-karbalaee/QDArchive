import sqlite3

def init_db(db_path="metadata.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create PROJECTS table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PROJECTS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string TEXT,
            repository_id INTEGER,
            repository_url TEXT,
            project_url TEXT,
            version TEXT,
            title TEXT,
            description TEXT,
            language TEXT,
            doi TEXT,
            upload_date DATE,
            download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            download_repository_folder TEXT,
            download_project_folder TEXT,
            download_version_folder TEXT,
            download_method TEXT
        )
    ''')

    # Create FILES table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FILES (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            file_name TEXT,
            file_type TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS (id)
        )
    ''')

    # Create KEYWORDS table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS KEYWORDS (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            keyword TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS (id)
        )
    ''')

    # Create PERSON_ROLE table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PERSON_ROLE (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            name TEXT,
            role TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS (id)
        )
    ''')

    # Create LICENSES table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS LICENSES (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER,
            license TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS (id)
        )
    ''')

    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {db_path}")

if __name__ == "__main__":
    init_db()