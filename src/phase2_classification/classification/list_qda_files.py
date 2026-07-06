import argparse
import csv
import sqlite3
from pathlib import Path

from rules import normalize_extension, QDA_EXTENSIONS


def get_qda_files(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, project_id, file_name, file_type FROM FILES")
        rows = cursor.fetchall()
    finally:
        conn.close()

    qda_files = []
    for file_id, project_id, file_name, file_type in rows:
        ext = normalize_extension(file_type, file_name)
        if ext in QDA_EXTENSIONS:
            qda_files.append((file_id, project_id, file_name, file_type))
    return qda_files


def main():
    parser = argparse.ArgumentParser(description="List all files in FILES with a known QDA file extension.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output", default="src/phase2_classification/classification/output/qda_files.csv")
    args = parser.parse_args()

    qda_files = get_qda_files(args.db)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["file_id", "project_id", "file_name", "file_type"])
        writer.writerows(qda_files)

    print(f"QDA files found: {len(qda_files)}")
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
