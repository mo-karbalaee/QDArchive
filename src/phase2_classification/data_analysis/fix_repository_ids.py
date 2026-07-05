import argparse
import sqlite3

from repository_labels import REPOSITORY_LABELS

FALLBACK_REPOSITORY_ID = 17


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def fix_repository_ids(conn):
    valid_ids = set(REPOSITORY_LABELS.keys())
    cursor = conn.cursor()
    cursor.execute("SELECT id, repository_id FROM PROJECTS")
    rows = cursor.fetchall()

    invalid = [(project_id, repository_id) for project_id, repository_id in rows if repository_id not in valid_ids]

    for project_id, _ in invalid:
        cursor.execute("UPDATE PROJECTS SET repository_id = ? WHERE id = ?", (FALLBACK_REPOSITORY_ID, project_id))

    conn.commit()
    return invalid


def main():
    parser = argparse.ArgumentParser(
        description="Reset repository_id to 17 (N/A) for projects outside the known 1-20 range."
    )
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    args = parser.parse_args()

    conn = get_connection(args.db)
    try:
        invalid = fix_repository_ids(conn)
    finally:
        conn.close()

    print(f"Updated {len(invalid)} project(s) to repository_id={FALLBACK_REPOSITORY_ID} (N/A):")
    for project_id, old_value in invalid:
        print(f"  project id {project_id}: {old_value} -> {FALLBACK_REPOSITORY_ID}")


if __name__ == "__main__":
    main()
