import argparse
import sqlite3
from pathlib import Path

from charts import plot_pie, plot_bar

PROJECT_TYPES = ("QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT")


def get_project_type_counts(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT type, COUNT(*) FROM PROJECTS GROUP BY type")
        rows = cursor.fetchall()
    finally:
        conn.close()

    counts = {}
    for project_type, count in rows:
        label = project_type if project_type in PROJECT_TYPES else (project_type or "UNCLASSIFIED")
        counts[label] = counts.get(label, 0) + count
    return counts


def main():
    parser = argparse.ArgumentParser(description="Plot the distribution of PROJECT_TYPE across all projects.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output-dir", default="src/phase2_classification/data_analysis/output")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = get_project_type_counts(args.db)

    pie_path = output_dir / "project_type_pie.png"
    histogram_path = output_dir / "project_type_histogram.png"
    plot_pie(counts, "Projects by type", pie_path, top_n=len(PROJECT_TYPES))
    plot_bar(counts, "Projects per type", "Number of projects", histogram_path)

    print(f"Project type counts ({len(counts)} types, {sum(counts.values())} projects):")
    for label, count in sorted(counts.items(), key=lambda pair: pair[1], reverse=True):
        print(f"  {label}: {count}")

    print(f"\nSaved: {pie_path}")
    print(f"Saved: {histogram_path}")


if __name__ == "__main__":
    main()
