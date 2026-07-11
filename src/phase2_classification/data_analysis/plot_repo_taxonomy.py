import argparse
import csv
import re
import sqlite3
from pathlib import Path

from charts import plot_bar_with_counts, plot_distribution_pie
from repository_labels import REPOSITORY_LABELS

TOP_N_TABLE = 20

PROJECT_TYPE_LABELS = {
    "QDA_PROJECT": "QDA",
    "QD_PROJECT": "QD",
    "OTHER_PROJECT": "Other project",
    "NOT_A_PROJECT": "No project",
}
PROJECT_TYPE_ORDER = ["QDA", "QD", "Other project", "No project"]

SCOPED_DISTRIBUTIONS = [
    ("qda", "QDA_PROJECT", "QDA"),
    ("qd", "QD_PROJECT", "QD"),
]


def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def get_repo_counts(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT repository_id, primary_class, type FROM PROJECTS")
        rows = cursor.fetchall()
    finally:
        conn.close()

    per_repo_classes = {}
    per_repo_types = {}
    per_repo_classes_by_type = {}
    for repository_id, primary_class, project_type in rows:
        repo_name = REPOSITORY_LABELS.get(repository_id, f"unknown ({repository_id})")

        class_label = primary_class or "UNCLASSIFIED"
        class_counts = per_repo_classes.setdefault(repo_name, {})
        class_counts[class_label] = class_counts.get(class_label, 0) + 1

        type_label = PROJECT_TYPE_LABELS.get(project_type, "No project")
        type_counts = per_repo_types.setdefault(repo_name, {})
        type_counts[type_label] = type_counts.get(type_label, 0) + 1

        scoped_classes = per_repo_classes_by_type.setdefault(repo_name, {})
        class_counts_for_type = scoped_classes.setdefault(project_type, {})
        class_counts_for_type[class_label] = class_counts_for_type.get(class_label, 0) + 1

    return per_repo_classes, per_repo_types, per_repo_classes_by_type


def write_table(counts, output_path, top_n=TOP_N_TABLE):
    ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)[:top_n]
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "primary_class", "count"])
        for rank, (label, count) in enumerate(ranked, 1):
            writer.writerow([rank, label, count])
    return ranked


def build_narrative(repo_name, total, distinct, ranked_top, type_counts):
    def pct(n):
        return (n / total * 100) if total else 0

    sentences = [
        f"The {repo_name} repository contributes {total} projects to the aggregated dataset."
    ]
    sentences.append(
        f"Of these, {pct(type_counts.get('QD', 0)):.1f}% were identified as QD projects and "
        f"{pct(type_counts.get('QDA', 0)):.1f}% as QDA projects, with "
        f"{pct(type_counts.get('Other project', 0)):.1f}% classified as other data and "
        f"{pct(type_counts.get('No project', 0)):.1f}% as non-project entries."
    )
    if ranked_top:
        top_label, top_count = ranked_top[0]
        sentences.append(
            f"Across {distinct} distinct ISIC divisions, the most frequent primary class is "
            f"{top_label}, accounting for {pct(top_count):.1f}% of the repository's projects."
        )
    return " ".join(sentences)


def write_comments(repo_name, counts, ranked_top, type_counts, output_path):
    total = sum(counts.values())
    distinct = len(counts)
    unclassified = counts.get("UNCLASSIFIED", 0)

    lines = [f"# Findings: {repo_name}", ""]
    lines.append(build_narrative(repo_name, total, distinct, ranked_top, type_counts))
    lines.append("")
    lines.append("## Key facts")
    lines.append("")
    lines.append(f"- Total projects: {total}")
    lines.append(f"- Distinct primary classes observed: {distinct}")
    if ranked_top:
        top_label, top_count = ranked_top[0]
        share = (top_count / total * 100) if total else 0
        lines.append(f"- Most common primary class: {top_label} ({top_count} projects, {share:.1f}%)")
    if unclassified:
        share = (unclassified / total * 100) if total else 0
        lines.append(f"- Unclassified projects (no primary_class): {unclassified} ({share:.1f}%)")

    lines.append("")
    lines.append("## Projects by type")
    lines.append("")
    for type_label in PROJECT_TYPE_ORDER:
        count = type_counts.get(type_label, 0)
        share = (count / total * 100) if total else 0
        lines.append(f"- {type_label}: {count} ({share:.1f}%)")

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

    per_repo_classes, per_repo_types, per_repo_classes_by_type = get_repo_counts(args.db)

    for repo_name in sorted(per_repo_classes):
        counts = per_repo_classes[repo_name]
        slug = slugify(repo_name)
        repo_dir = output_root / slug
        repo_dir.mkdir(parents=True, exist_ok=True)

        histogram_path = repo_dir / f"{slug}_primary_class_histogram.svg"
        plot_bar_with_counts(counts, f"Primary classes - {repo_name}", "Number of projects", histogram_path)

        table_path = repo_dir / "primary_class_table.csv"
        ranked_top = write_table(counts, table_path)

        type_counts = per_repo_types[repo_name]

        class_pie_path = repo_dir / f"{slug}_primary_class_pie.svg"
        plot_distribution_pie(counts, f"Primary classes - {repo_name}", class_pie_path, top_n=7)

        type_pie_counts = {label: type_counts.get(label, 0) for label in PROJECT_TYPE_ORDER}
        type_pie_path = repo_dir / f"{slug}_project_type_pie.svg"
        plot_distribution_pie(type_pie_counts, f"Project types - {repo_name}", type_pie_path, top_n=7, donut=True)

        comments_path = repo_dir / "comments.md"
        write_comments(repo_name, counts, ranked_top, type_counts, comments_path)

        scoped_classes = per_repo_classes_by_type.get(repo_name, {})
        for suffix, project_type, human_label in SCOPED_DISTRIBUTIONS:
            scoped_counts = scoped_classes.get(project_type, {})
            if not scoped_counts:
                continue
            scoped_histogram_path = repo_dir / f"{slug}_primary_class_histogram_{suffix}.svg"
            plot_bar_with_counts(
                scoped_counts,
                f"Primary classes ({human_label}) - {repo_name}",
                "Number of projects",
                scoped_histogram_path,
            )
            scoped_table_path = repo_dir / f"primary_class_table_{suffix}.csv"
            write_table(scoped_counts, scoped_table_path)

        print(f"{repo_name}: {sum(counts.values())} projects, {len(counts)} classes -> {repo_dir}")


if __name__ == "__main__":
    main()
