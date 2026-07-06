import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

from schema import (
    PROJECT_TYPE_VALUES,
    DOWNLOAD_METHOD_VALUES,
    DOWNLOAD_RESULT_VALUES,
    PERSON_ROLE_VALUES,
    create_table_statements,
)

VIOLATION_CHECKS = [
    ("PROJECTS", "repository_id IS NULL", "PROJECTS.repository_id is NULL"),
    ("PROJECTS", "repository_url IS NULL", "PROJECTS.repository_url is NULL"),
    ("PROJECTS", "project_url IS NULL", "PROJECTS.project_url is NULL"),
    ("PROJECTS", "title IS NULL", "PROJECTS.title is NULL"),
    ("PROJECTS", "description IS NULL", "PROJECTS.description is NULL"),
    ("PROJECTS", "download_date IS NULL", "PROJECTS.download_date is NULL"),
    ("PROJECTS", "download_repository_folder IS NULL", "PROJECTS.download_repository_folder is NULL"),
    ("PROJECTS", "download_project_folder IS NULL", "PROJECTS.download_project_folder is NULL"),
    ("PROJECTS", "download_method IS NULL", "PROJECTS.download_method is NULL"),
    (
        "PROJECTS",
        f"download_method IS NOT NULL AND download_method NOT IN ({', '.join(repr(v) for v in DOWNLOAD_METHOD_VALUES)})",
        "PROJECTS.download_method has a value outside SCRAPING/API-CALL",
    ),
    (
        "PROJECTS",
        f"type IS NOT NULL AND type NOT IN ({', '.join(repr(v) for v in PROJECT_TYPE_VALUES)})",
        "PROJECTS.type has a value outside the PROJECT_TYPE enum",
    ),
    ("FILES", "file_name IS NULL", "FILES.file_name is NULL"),
    ("FILES", "file_type IS NULL", "FILES.file_type is NULL"),
    ("FILES", "status IS NULL", "FILES.status is NULL"),
    (
        "FILES",
        f"status IS NOT NULL AND status NOT IN ({', '.join(repr(v) for v in DOWNLOAD_RESULT_VALUES)})",
        "FILES.status has a value outside the DOWNLOAD_RESULT enum",
    ),
    ("KEYWORDS", "keyword IS NULL", "KEYWORDS.keyword is NULL"),
    ("PERSON_ROLE", "name IS NULL", "PERSON_ROLE.name is NULL"),
    ("PERSON_ROLE", "role IS NULL", "PERSON_ROLE.role is NULL"),
    (
        "PERSON_ROLE",
        f"role IS NOT NULL AND role NOT IN ({', '.join(repr(v) for v in PERSON_ROLE_VALUES)})",
        "PERSON_ROLE.role has a value outside the PERSON_ROLE enum",
    ),
    ("LICENSES", "license IS NULL", "LICENSES.license is NULL"),
]

DUPLICATE_URL_CHECK = '''
    SELECT project_url, COUNT(*) FROM PROJECTS
    WHERE project_url IS NOT NULL
    GROUP BY project_url
    HAVING COUNT(*) > 1
'''

REBUILD_STATEMENTS = [
    ("PROJECTS", [
        "query_string", "repository_id", "repository_url", "project_url", "version",
        "type", "title", "description", "language", "doi", "upload_date", "download_date",
        "download_repository_folder", "download_project_folder", "download_version_folder",
        "download_method", "primary_class", "secondary_class", "tags",
    ]),
    ("FILES", ["project_id", "file_name", "file_type", "status"]),
    ("KEYWORDS", ["project_id", "keyword"]),
    ("PERSON_ROLE", ["project_id", "name", "role"]),
    ("LICENSES", ["project_id", "license"]),
]


def find_violations(conn):
    violations = []
    cursor = conn.cursor()
    table_columns = {
        table: {row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()}
        for table in {table for table, _, _ in VIOLATION_CHECKS}
    }

    for table, condition, description in VIOLATION_CHECKS:
        referenced_column = condition.split()[0]
        if referenced_column not in table_columns[table]:
            continue
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
        count = cursor.fetchone()[0]
        if count:
            violations.append(f"{description}: {count} row(s)")

    cursor.execute(DUPLICATE_URL_CHECK)
    duplicates = cursor.fetchall()
    if duplicates:
        violations.append(f"PROJECTS.project_url has {len(duplicates)} duplicate value(s) (would break UNIQUE)")

    return violations


def existing_columns(cursor, table):
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def rebuild(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF")

    old_columns = {table: existing_columns(cursor, table) for table, _ in REBUILD_STATEMENTS}

    for table, _ in REBUILD_STATEMENTS:
        cursor.execute(f"ALTER TABLE {table} RENAME TO {table}_OLD")

    for statement in create_table_statements():
        cursor.execute(statement)

    for table, columns in REBUILD_STATEMENTS:
        target_columns = ", ".join(columns)
        source_columns = ", ".join(
            column if column in old_columns[table] else "NULL"
            for column in columns
        )
        cursor.execute(f"INSERT INTO {table} ({target_columns}) SELECT {source_columns} FROM {table}_OLD")

    for table, _ in REBUILD_STATEMENTS:
        cursor.execute(f"DROP TABLE {table}_OLD")

    cursor.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate the classification database to the tightened schema (NOT NULL + CHECK constraints)."
    )
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--apply", action="store_true", help="Actually perform the migration. Without this flag, only checks for violations.")
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        violations = find_violations(conn)
    finally:
        conn.close()

    if violations:
        print("Found constraint violations that would block migration:")
        for violation in violations:
            print(f"  - {violation}")
        print("\nResolve these first, or decide how to handle them, before migrating.")
        return

    print("No constraint violations found. The current data satisfies the tightened schema.")

    if not args.apply:
        print("Dry run only (pass --apply to actually migrate).")
        return

    backup_path = db_path.with_name(f"{db_path.stem}.backup-{datetime.now():%Y%m%dT%H%M%S}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    print(f"Backup written to: {backup_path}")

    conn = sqlite3.connect(db_path)
    try:
        rebuild(conn)
    finally:
        conn.close()

    print("Migration complete.")


if __name__ == "__main__":
    main()
