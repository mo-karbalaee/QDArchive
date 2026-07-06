import argparse
import sqlite3
from pathlib import Path

from charts import plot_pie, plot_bar


def get_class_counts(db_path, column):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT {column}, COUNT(*) FROM PROJECTS GROUP BY {column}")
        rows = cursor.fetchall()
    finally:
        conn.close()

    counts = {}
    for value, count in rows:
        label = value or "UNCLASSIFIED"
        counts[label] = counts.get(label, 0) + count
    return counts


def report(column, counts):
    print(f"{column} counts ({len(counts)} distinct values, {sum(counts.values())} projects):")
    for label, count in sorted(counts.items(), key=lambda pair: pair[1], reverse=True):
        print(f"  {label}: {count}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Plot the distribution of primary_class and secondary_class across all projects.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output-dir", default="src/phase2_classification/data_analysis/output")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for column, prefix, title_noun in (
        ("primary_class", "primary_class", "primary class"),
        ("secondary_class", "secondary_class", "secondary class"),
    ):
        counts = get_class_counts(args.db, column)
        report(column, counts)

        pie_path = output_dir / f"{prefix}_pie.svg"
        histogram_path = output_dir / f"{prefix}_histogram.svg"
        plot_pie(counts, f"Projects by {title_noun} (top 7 + Other)", pie_path)
        plot_bar(counts, f"Projects per {title_noun}", "Number of projects", histogram_path)

        print(f"Saved: {pie_path}")
        print(f"Saved: {histogram_path}\n")


if __name__ == "__main__":
    main()
