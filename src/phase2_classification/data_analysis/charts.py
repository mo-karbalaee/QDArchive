import matplotlib.pyplot as plt

CATEGORICAL_COLORS = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]
SEQUENTIAL_COLOR = "#2a78d6"
SURFACE_COLOR = "#fcfcfb"
PRIMARY_INK = "#0b0b0b"
MUTED_INK = "#898781"
GRIDLINE_COLOR = "#e1e0d9"
TOP_N_SLICES = 7


def fold_into_other(counts, top_n=TOP_N_SLICES):
    ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)
    if len(ranked) <= top_n:
        return ranked
    kept = ranked[:top_n]
    other_total = sum(count for _, count in ranked[top_n:])
    kept.append(("Other", other_total))
    return kept


def plot_pie(counts, title, output_path, top_n=TOP_N_SLICES):
    slices = fold_into_other(counts, top_n=top_n)
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
    ax.set_title(title, color=PRIMARY_INK)
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR, bbox_inches="tight")
    plt.close(fig)


def _shorten(label, limit=34):
    if len(label) <= limit:
        return label
    return label[: limit - 1].rstrip() + "…"


def plot_distribution_pie(counts, title, output_path, top_n=7, donut=False):
    slices = [pair for pair in fold_into_other(counts, top_n=top_n) if pair[1] > 0]
    labels = [_shorten(label) for label, _ in slices]
    values = [count for _, count in slices]
    colors = CATEGORICAL_COLORS[: len(slices)]

    background = "#ffffff"
    fig, ax = plt.subplots(figsize=(6, 6), facecolor=background)
    ax.set_facecolor(background)
    wedge_kw = {"linewidth": 2, "edgecolor": background}
    if donut:
        wedge_kw["width"] = 0.42
    wedges, _, _ = ax.pie(
        values,
        colors=colors,
        autopct="%1.1f%%",
        pctdistance=0.82,
        startangle=90,
        textprops={"color": PRIMARY_INK, "fontsize": 9},
        wedgeprops=wedge_kw,
    )
    ax.set_title(title, color=PRIMARY_INK)
    ax.legend(
        wedges,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.02),
        frameon=False,
        fontsize=8,
        labelcolor=PRIMARY_INK,
    )
    ax.axis("equal")
    fig.savefig(output_path, format="svg", facecolor=background, bbox_inches="tight")
    plt.close(fig)


def plot_bar(counts, title, xlabel, output_path):
    ranked = sorted(counts.items(), key=lambda pair: pair[1])
    labels = [label for label, _ in ranked]
    values = [count for _, count in ranked]

    fig, ax = plt.subplots(figsize=(9, max(4, len(labels) * 0.4)), facecolor=SURFACE_COLOR)
    ax.set_facecolor(SURFACE_COLOR)
    ax.barh(labels, values, color=SEQUENTIAL_COLOR)
    ax.set_xlabel(xlabel, color=MUTED_INK)
    ax.set_title(title, color=PRIMARY_INK)
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
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR, bbox_inches="tight")
    plt.close(fig)


def plot_bar_with_counts(counts, title, ylabel, output_path, top_n=20):
    ranked = sorted(counts.items(), key=lambda pair: pair[1], reverse=True)[:top_n]
    ranked = ranked[::-1]
    labels = [label for label, _ in ranked]
    values = [count for _, count in ranked]
    max_value = max(values) if values else 0

    background = "#ffffff"
    fig, ax = plt.subplots(figsize=(11, 5), facecolor=background)
    ax.set_facecolor(background)
    bars = ax.barh(labels, values, color=SEQUENTIAL_COLOR)
    ax.set_xlabel(ylabel, color=MUTED_INK)
    ax.set_title(title, color=PRIMARY_INK)
    ax.tick_params(colors=MUTED_INK)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(GRIDLINE_COLOR)
    ax.spines["bottom"].set_color(GRIDLINE_COLOR)
    ax.xaxis.grid(True, color=GRIDLINE_COLOR, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlim(0, max_value * 1.12 if max_value else 1)
    plt.setp(ax.get_yticklabels(), color=PRIMARY_INK, fontsize=7)
    for label in ax.get_xticklabels():
        label.set_color(PRIMARY_INK)

    for bar, value in zip(bars, values):
        ax.text(
            bar.get_width() + max_value * 0.01,
            bar.get_y() + bar.get_height() / 2,
            str(value),
            ha="left",
            va="center",
            color=PRIMARY_INK,
            fontsize=7,
        )

    fig.tight_layout()
    fig.savefig(output_path, format="svg", facecolor=background, bbox_inches="tight")
    plt.close(fig)
