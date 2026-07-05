import argparse
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt

from repository_labels import REPOSITORY_LABELS

CATEGORICAL_COLORS = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]
SEQUENTIAL_COLOR = "#2a78d6"
SURFACE_COLOR = "#fcfcfb"
PRIMARY_INK = "#0b0b0b"
MUTED_INK = "#898781"
GRIDLINE_COLOR = "#e1e0d9"
TOP_N_SLICES = 7


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


def fold_into_other(counts, top_n=TOP_N_SLICES):
    ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)
    if len(ranked) <= top_n:
        return ranked
    kept = ranked[:top_n]
    other_total = sum(count for _, count in ranked[top_n:])
    kept.append(("Other", other_total))
    return kept


def plot_pie(counts, output_path):
    slices = fold_into_other(counts)
    labels = [label for label, _ in slices]
    values = [count for _, count in slices]
    colors = CATEGORICAL_COLORS[:len(slices)]

    fig, ax = plt.subplots(figsize=(8, 8), facecolor=SURFACE_COLOR)
    ax.set_facecolor(SURFACE_COLOR)
    ax.pie(
        values,
        labels=labels,
        colors=colors,
        autopct="%1.0f%%",
        pctdistance=0.8,
        textprops={"color": PRIMARY_INK},
        wedgeprops={"linewidth": 2, "edgecolor": SURFACE_COLOR},
    )
    ax.set_title("Projects by repository (top 7 + Other)", color=PRIMARY_INK)
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR)
    plt.close(fig)


def plot_histogram(counts, output_path):
    ranked = sorted(counts.items(), key=lambda pair: pair[1])
    labels = [label for label, _ in ranked]
    values = [count for _, count in ranked]

    fig, ax = plt.subplots(figsize=(9, max(4, len(labels) * 0.4)), facecolor=SURFACE_COLOR)
    ax.set_facecolor(SURFACE_COLOR)
    ax.barh(labels, values, color=SEQUENTIAL_COLOR)
    ax.set_xlabel("Number of projects", color=MUTED_INK)
    ax.set_title("Projects per repository", color=PRIMARY_INK)
    ax.tick_params(colors=MUTED_INK)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(GRIDLINE_COLOR)
    ax.spines["bottom"].set_color(GRIDLINE_COLOR)
    ax.xaxis.grid(True, color=GRIDLINE_COLOR, linewidth=0.8)
    ax.set_axisbelow(True)
    for label in ax.get_yticklabels():
        label.set_color(PRIMARY_INK)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot the distribution of projects across source repositories.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--output-dir", default="src/phase2_classification/data_analysis/output")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = get_repository_counts(args.db)

    pie_path = output_dir / "repository_pie.png"
    histogram_path = output_dir / "repository_histogram.png"
    plot_pie(counts, pie_path)
    plot_histogram(counts, histogram_path)

    print(f"Repository counts ({len(counts)} repositories, {sum(counts.values())} projects):")
    for label, count in sorted(counts.items(), key=lambda pair: pair[1], reverse=True):
        print(f"  {label}: {count}")

    print(f"\nSaved: {pie_path}")
    print(f"Saved: {histogram_path}")


if __name__ == "__main__":
    main()
