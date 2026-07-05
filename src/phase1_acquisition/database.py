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
        """Initializes the database schema with mandatory field constraints."""
        with self._get_cursor() as cursor:
            # 1. PROJECTS Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS PROJECTS (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_string TEXT,
                    repository_id INTEGER NOT NULL,
                    repository_url TEXT NOT NULL,
                    project_url TEXT UNIQUE NOT NULL, 
                    version TEXT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    language TEXT,
                    doi TEXT,              
                    upload_date DATE,
                    download_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    download_repository_folder TEXT NOT NULL,
                    download_project_folder TEXT NOT NULL,
                    download_version_folder TEXT,
                    download_method TEXT NOT NULL
                )
            ''')

            # 2. FILES Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS FILES (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 3. KEYWORDS Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS KEYWORDS (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    keyword TEXT NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 4. PERSON_ROLE Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS PERSON_ROLE (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

            # 5. LICENSES Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS LICENSES (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id INTEGER NOT NULL,
                    license TEXT NOT NULL,
                    FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
                )
            ''')

    def project_exists(self, project_url):
        """Checks if a Project URL already exists."""
        with self._get_cursor() as cursor:
            cursor.execute('SELECT id FROM PROJECTS WHERE project_url = ?', (project_url,))
            return cursor.fetchone() is not None

    def insert_project_data(self, project_info, files=None, keywords=None, people=None, licenses=None):
        """
        Inserts a project record and all related metadata.
        Required fields must be present in project_info to avoid NOT NULL constraints.
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
                project_info.get('repository_id'),
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
                project_info.get('download_method')
            ))
            
            project_id = cursor.lastrowid

            # Insert Files (includes mandatory status)
            if files:
                cursor.executemany(
                    'INSERT INTO FILES (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)',
                    [(project_id, f['name'], f['type'], f.get('status', 'SUCCEEDED')) for f in files]
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