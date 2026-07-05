import sqlite3
from contextlib import contextmanager

CANONICAL_ENTITIES = {
    "projects": {
        "columns": [
            "query_string", "repository_id", "repository_url", "project_url",
            "version", "title", "description", "language", "doi", "upload_date",
            "download_date", "download_repository_folder", "download_project_folder",
            "download_version_folder", "download_method",
        ],
    },
    "files": {
        "columns": ["file_name", "file_type", "status"],
        "parent_column": "project_id",
    },
    "keywords": {
        "columns": ["keyword"],
        "parent_column": "project_id",
    },
    "person_role": {
        "columns": ["name", "role"],
        "parent_column": "project_id",
    },
    "licenses": {
        "columns": ["license"],
        "parent_column": "project_id",
    },
}


@contextmanager
def get_cursor(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_output_db(db_path):
    with get_cursor(db_path) as cursor:
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
                download_date TIMESTAMP,
                download_repository_folder TEXT,
                download_project_folder TEXT,
                download_version_folder TEXT,
                download_method TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS FILES (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                file_name TEXT,
                file_type TEXT,
                status TEXT,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS KEYWORDS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                keyword TEXT,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS PERSON_ROLE (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                name TEXT,
                role TEXT,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS LICENSES (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                license TEXT,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''')
