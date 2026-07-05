import argparse
import sqlite3
import sys

from rules import classify_project

PROJECT_TYPES = ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT")


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_type_column(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(PROJECTS)")
    columns = [row[1] for row in cursor.fetchall()]
    if "type" not in columns:
        cursor.execute("ALTER TABLE PROJECTS ADD COLUMN type TEXT")
        conn.commit()


def classify_all(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM PROJECTS")
    project_ids = [row[0] for row in cursor.fetchall()]
    total = len(project_ids)

    tallies = {project_type: 0 for project_type in PROJECT_TYPES}

    for index, project_id in enumerate(project_ids, 1):
        cursor.execute("SELECT file_name, file_type FROM FILES WHERE project_id = ?", (project_id,))
        files = [{"file_name": row[0], "file_type": row[1]} for row in cursor.fetchall()]
        project_type = classify_project(files)
        cursor.execute("UPDATE PROJECTS SET type = ? WHERE id = ?", (project_type, project_id))
        tallies[project_type] += 1

        sys.stdout.write(f"\rClassifying project {index}/{total} ({index * 100 // total}%)")
        sys.stdout.flush()

    sys.stdout.write("\n")
    conn.commit()
    return tallies


def main():
    parser = argparse.ArgumentParser(description="Classify each project's PROJECT_TYPE.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    args = parser.parse_args()

    conn = get_connection(args.db)
    try:
        ensure_type_column(conn)
        tallies = classify_all(conn)
    finally:
        conn.close()

    print("Project type tallies:")
    for project_type, count in tallies.items():
        print(f"  {project_type}: {count}")


if __name__ == "__main__":
    main()
