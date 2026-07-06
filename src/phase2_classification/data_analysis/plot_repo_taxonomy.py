import argparse
import csv
import re
import sqlite3
from pathlib import Path

from charts import plot_bar_with_counts
from repository_labels import REPOSITORY_LABELS

TOP_N_TABLE = 20


def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def get_repo_class_counts(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT repository_id, primary_class FROM PROJECTS")
        rows = cursor.fetchall()
    finally:
        conn.close()

    per_repo = {}
    for repository_id, primary_class in rows:
        repo_name = REPOSITORY_LABELS.get(repository_id, f"unknown ({repository_id})")
        label = primary_class or "UNCLASSIFIED"
        counts = per_repo.setdefault(repo_name, {})
        counts[label] = counts.get(label, 0) + 1
    return per_repo


def write_table(counts, output_path, top_n=TOP_N_TABLE):
    ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)[:top_n]
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "primary_class", "count"])
        for rank, (label, count) in enumerate(ranked, 1):
            writer.writerow([rank, label, count])
    return ranked


def write_comments(repo_name, counts, ranked_top, output_path):
    total = sum(counts.values())
    distinct = len(counts)
    unclassified = counts.get("UNCLASSIFIED", 0)

    lines = [f"# Findings: {repo_name}", ""]
    lines.append(f"- Total projects: {total}")
    lines.append(f"- Distinct primary classes observed: {distinct}")
    if ranked_top:
        top_label, top_count = ranked_top[0]
        share = (top_count / total * 100) if total else 0
        lines.append(f"- Most common primary class: {top_label} ({top_count} projects, {share:.1f}%)")
    if unclassified:
        share = (unclassified / total * 100) if total else 0
        lines.append(f"- Unclassified projects (no primary_class): {unclassified} ({share:.1f}%)")

    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Per-repository primary class histograms, rank tables, and findings."
    )
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output-dir", default="src/phase2_classification/data_analysis/output/by_repository")
    args = parser.parse_args()

    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    per_repo = get_repo_class_counts(args.db)

    for repo_name in sorted(per_repo):
        counts = per_repo[repo_name]
        repo_dir = output_root / slugify(repo_name)
        repo_dir.mkdir(parents=True, exist_ok=True)

        histogram_path = repo_dir / "primary_class_histogram.svg"
        plot_bar_with_counts(counts, f"Primary classes - {repo_name}", "Number of projects", histogram_path)

        table_path = repo_dir / "primary_class_table.csv"
        ranked_top = write_table(counts, table_path)

        comments_path = repo_dir / "comments.md"
        write_comments(repo_name, counts, ranked_top, comments_path)

        print(f"{repo_name}: {sum(counts.values())} projects, {len(counts)} classes -> {repo_dir}")


if __name__ == "__main__":
    main()
