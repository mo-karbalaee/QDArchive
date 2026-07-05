import argparse
import sqlite3
from pathlib import Path

from repository_labels import REPOSITORY_LABELS
from charts import plot_pie, plot_bar


def get_repository_counts(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT repository_id, COUNT(*) FROM PROJECTS GROUP BY repository_id")
        rows = cursor.fetchall()
    finally:
        conn.close()

    counts = {}
    for repository_id, count in rows:
        label = REPOSITORY_LABELS.get(repository_id, f"unknown ({repository_id})")
        counts[label] = counts.get(label, 0) + count
    return counts


def main():
    parser = argparse.ArgumentParser(description="Plot the distribution of projects across source repositories.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output-dir", default="src/phase2_classification/data_analysis/output")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = get_repository_counts(args.db)

    pie_path = output_dir / "repository_pie.svg"
    histogram_path = output_dir / "repository_histogram.svg"
    plot_pie(counts, "Projects by repository (top 7 + Other)", pie_path)
    plot_bar(counts, "Projects per repository", "Number of projects", histogram_path)

    print(f"Repository counts ({len(counts)} repositories, {sum(counts.values())} projects):")
    for label, count in sorted(counts.items(), key=lambda pair: pair[1], reverse=True):
        print(f"  {label}: {count}")

    print(f"\nSaved: {pie_path}")
    print(f"Saved: {histogram_path}")


if __name__ == "__main__":
    main()
