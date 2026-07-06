import argparse
import csv
import sqlite3
from pathlib import Path

QUERY = """
    SELECT
        p.repository_id,
        p.type,
        p.title,
        p.primary_class,
        p.secondary_class,
        COUNT(f.id)
    FROM PROJECTS p
    LEFT JOIN FILES f ON f.project_id = p.id
    GROUP BY p.id
"""


def get_projects_summary(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY)
        return cursor.fetchall()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Export a flat per-project summary table.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output", default="src/phase2_classification/data_analysis/output/projects_summary.csv")
    args = parser.parse_args()

    rows = get_projects_summary(args.db)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "repository_id",
            "project_type",
            "project_title",
            "primary_class",
            "secondary_class",
            "no_project_files",
        ])
        writer.writerows(rows)

    print(f"Projects exported: {len(rows)}")
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
