import sqlite3
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self, db_path="23688981-seeding.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _get_cursor(self):
        """Context manager to handle connection and transactions automatically."""
        conn = sqlite3.connect(self.db_path)
        # Enable foreign key support in SQLite
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _init_db(self):
        """Initializes the database schema based on the provided specification."""
        with self._get_cursor() as cursor:
            # 1. PROJECTS Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS PROJECTS (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_string TEXT,
                    repository_id INTEGER,
                    repository_url TEXT,
                    project_url TEXT UNIQUE, 
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

            # 2. FILES Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS FILES (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER,
                    file_name TEXT,
                    file_type TEXT,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 3. KEYWORDS Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS KEYWORDS (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER,
                    keyword TEXT,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 4. PERSON_ROLE Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS PERSON_ROLE (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER,
                    name TEXT,
                    role TEXT,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 5. LICENSES Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS LICENSES (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER,
                    license TEXT,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

    def project_exists(self, project_url):
        """Checks if a Project URL already exists in the database."""
        with self._get_cursor() as cursor:
            cursor.execute('SELECT id FROM PROJECTS WHERE project_url = ?', (project_url,))
            return cursor.fetchone() is not None

    def insert_project_data(self, project_info, files=None, keywords=None, people=None, licenses=None):
        """
        Inserts a full project record and all related metadata in a single transaction.
        
        :param project_info: dict containing keys matching PROJECTS columns
        :param files: list of dicts [{'name': ..., 'type': ...}]
        :param keywords: list of strings
        :param people: list of dicts [{'name': ..., 'role': ...}]
        :param licenses: list of strings
        """
        files = files or []
        keywords = keywords or []
        people = people or []
        licenses = licenses or []

        with self._get_cursor() as cursor:
            # Insert Main Project
            cursor.execute('''
                INSERT INTO PROJECTS (
                    query_string, repository_id, repository_url, project_url, 
                    version, title, description, language, doi, upload_date,
                    download_repository_folder, download_project_folder, 
                    download_version_folder, download_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                project_info.get('query_string'),
                project_info.get('repository_id', 1), # Default to 1 for Harvard
                project_info.get('repository_url'),
                project_info.get('project_url'),
                project_info.get('version'),
                project_info.get('title'),
                project_info.get('description'),
                project_info.get('language'),
                project_info.get('doi'),
                project_info.get('upload_date'),
                project_info.get('download_repository_folder'),
                project_info.get('download_project_folder'),
                project_info.get('download_version_folder'),
                project_info.get('download_method', 'API-CALL')
            ))
            
            project_id = cursor.lastrowid

            # Insert Files
            if files:
                cursor.executemany(
                    'INSERT INTO FILES (project_id, file_name, file_type) VALUES (?, ?, ?)',
                    [(project_id, f['name'], f['type']) for f in files]
                )

            # Insert Keywords
            if keywords:
                cursor.executemany(
                    'INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)',
                    [(project_id, kw) for kw in keywords]
                )

            # Insert People
            if people:
                cursor.executemany(
                    'INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)',
                    [(project_id, p['name'], p['role']) for p in people]
                )

            # Insert Licenses
            if licenses:
                cursor.executemany(
                    'INSERT INTO LICENSES (project_id, license) VALUES (?, ?)',
                    [(project_id, lic) for lic in licenses]
                )

            return project_id