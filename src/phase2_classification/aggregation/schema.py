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


PROJECT_TYPE_VALUES = ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT")
DOWNLOAD_METHOD_VALUES = ("SCRAPING", "API-CALL")
DOWNLOAD_RESULT_VALUES = ("SUCCEEDED", "FAILED_SERVER_UNRESPONSIVE", "FAILED_LOGIN_REQUIRED", "FAILED_TOO_LARGE")
PERSON_ROLE_VALUES = ("UPLOADER", "AUTHOR", "OWNER", "OTHER", "UNKNOWN")


def _sql_list(values):
    return ", ".join(f"'{value}'" for value in values)


def create_table_statements():
    return [
        f'''
            CREATE TABLE IF NOT EXISTS PROJECTS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_string TEXT,
                repository_id INTEGER NOT NULL,
                repository_url TEXT NOT NULL,
                project_url TEXT UNIQUE NOT NULL,
                version TEXT,
                type TEXT CHECK (type IN ({_sql_list(PROJECT_TYPE_VALUES)})),
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                language TEXT,
                doi TEXT,
                upload_date DATE,
                download_date TIMESTAMP NOT NULL,
                download_repository_folder TEXT NOT NULL,
                download_project_folder TEXT NOT NULL,
                download_version_folder TEXT,
                download_method TEXT NOT NULL CHECK (download_method IN ({_sql_list(DOWNLOAD_METHOD_VALUES)})),
                primary_class TEXT,
                secondary_class TEXT,
                tags TEXT
            )
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS FILES (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_type TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ({_sql_list(DOWNLOAD_RESULT_VALUES)})),
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''',
        '''
            CREATE TABLE IF NOT EXISTS KEYWORDS (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS PERSON_ROLE (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ({_sql_list(PERSON_ROLE_VALUES)})),
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''',
        '''
            CREATE TABLE IF NOT EXISTS LICENSES (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                license TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES PROJECTS (id) ON DELETE CASCADE
            )
        ''',
    ]


def init_output_db(db_path):
    with get_cursor(db_path) as cursor:
        for statement in create_table_statements():
            cursor.execute(statement)
