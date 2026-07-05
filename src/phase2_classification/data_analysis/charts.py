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
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR)
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
    fig.savefig(output_path, dpi=150, facecolor=SURFACE_COLOR)
    plt.close(fig)
