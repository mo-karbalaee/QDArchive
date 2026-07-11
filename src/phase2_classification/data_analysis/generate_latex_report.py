import argparse
import csv
from pathlib import Path

REPO_SECTIONS = [
    ("Zenodo", "zenodo"),
    ("Harvard Dataverse", "harvard-dataverse"),
    ("DANS", "dans"),
    ("Dryad", "dryad"),
    ("Open Data Uni Halle", "open-data-uni-halle"),
    ("Finnish Social Science Data Archive", "finnish-social-science-data-archive"),
    ("UK Data Service", "uk-data-service"),
    ("Syracuse Qualitative Data Repository", "syracuse-qualitative-data-repository"),
    ("SIKT", "sikt"),
    ("CESSDA", "cessda"),
    ("ICPSR", "icpsr"),
    ("ADA", "ada"),
    ("Harvard Murray Archive", "harvard-murray-archive"),
    ("DataverseNO", "dataverse-no"),
    ("Columbia Oral History Archive", "columbia-oral-history-archive"),
    ("HIHSN", "hihsn"),
    ("SADA", "sada"),
    ("N/A", "n-a"),
    ("AUSSDA", "aussda"),
]

SCOPED_DISTRIBUTIONS = [
    ("qda", "QDA_PROJECT"),
    ("qd", "QD_PROJECT"),
]

INTRO = r"""\section{Classification Results}

In this section, I present the results of the filtering and classification procedures applied to the aggregated database. The analysis is structured on a per-repository basis in order to provide a clear and systematic overview of the observed patterns. Each repository-specific subsection contains a set of standardized outputs designed to facilitate comparison across repositories and to support interpretation of the classification results.

More specifically, each subsection includes the following components:

\begin{enumerate}

    \item A histogram showing the distribution of primary classes detected in the repository. This allows for a detailed examination of the most prevalent classification outcomes and highlights dominant patterns in the data.

    \item A rank-ordered table listing the identified classes according to their frequency. This table provides a compact summary of class prevalence and supports direct comparison between classes in terms of their occurrence.
\end{enumerate}
"""

LATEX_SPECIAL_CHARS = {
    "\\": r"\textbackslash{}",
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}


def escape_latex(text):
    return "".join(LATEX_SPECIAL_CHARS.get(char, char) for char in text)


def read_table(table_path):
    with open(table_path, newline="") as f:
        reader = csv.DictReader(f)
        return [(row["rank"], row["primary_class"], row["count"]) for row in reader]


def render_comments(comments_path):
    lines = []
    in_itemize = False
    for raw_line in Path(comments_path).read_text().splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("# "):
            continue
        if stripped.startswith("## "):
            if in_itemize:
                lines.append(r"\end{itemize}")
                in_itemize = False
            lines.append(rf"\textbf{{{escape_latex(stripped[3:])}}}")
            continue
        if stripped.startswith("- "):
            if not in_itemize:
                lines.append(r"\begin{itemize}")
                in_itemize = True
            lines.append(rf"    \item {escape_latex(stripped[2:])}")
            continue
        if not stripped:
            continue
        if in_itemize:
            lines.append(r"\end{itemize}")
            in_itemize = False
        lines.append(escape_latex(stripped))

    if in_itemize:
        lines.append(r"\end{itemize}")

    return "\n".join(lines)


def build_figure_latex(res_dir, slug, stem, caption):
    return "\n".join(
        [
            r"\begin{figure}[H]",
            r"    \centering",
            rf"    \includesvg[width=\textwidth]{{{res_dir}/{slug}/{stem}}}",
            rf"    \caption{{{caption}}}",
            r"\end{figure}",
        ]
    )


def build_table_latex(table_rows, caption):
    lines = [
        r"\begin{table}[H]",
        r"    \centering",
        r"    \begin{tabular}{r p{10cm} r}",
        r"    \hline",
        r"    \textbf{Rank} & \textbf{Primary Class} & \textbf{Count} \\",
        r"    \hline",
    ]
    for rank, primary_class, count in table_rows:
        lines.append(f"    {rank} & {escape_latex(primary_class)} & {count} \\\\")
    lines += [
        r"    \hline",
        r"    \end{tabular}",
        rf"    \caption{{{caption}}}",
        r"\end{table}",
    ]
    return "\n".join(lines)


def build_overview_figure(title, slug, res_dir):
    return "\n".join(
        [
            r"\begin{figure}[H]",
            r"    \centering",
            r"    \begin{subfigure}[t]{0.49\textwidth}",
            r"        \centering",
            rf"        \includesvg[width=\textwidth]{{{res_dir}/{slug}/{slug}_primary_class_pie}}",
            r"        \caption{Primary class distribution (all projects)}",
            r"    \end{subfigure}",
            r"    \hfill",
            r"    \begin{subfigure}[t]{0.49\textwidth}",
            r"        \centering",
            rf"        \includesvg[width=\textwidth]{{{res_dir}/{slug}/{slug}_project_type_pie}}",
            r"        \caption{Project type distribution}",
            r"    \end{subfigure}",
            rf"    \caption{{Repository overview --- {escape_latex(title)}}}",
            r"\end{figure}",
        ]
    )


def build_histograms_figure(title, slug, res_dir, repo_dir):
    lines = [
        r"\begin{figure}[H]",
        r"    \centering",
        rf"    \includesvg[width=\textwidth]{{{res_dir}/{slug}/{slug}_primary_class_histogram}}",
    ]
    for suffix, type_label in SCOPED_DISTRIBUTIONS:
        label = escape_latex(type_label)
        histogram_path = repo_dir / f"{slug}_primary_class_histogram_{suffix}.svg"
        lines.append(r"    \par\vspace{0.8em}")
        if histogram_path.exists():
            lines.append(
                rf"    \includesvg[width=\textwidth]{{{res_dir}/{slug}/{slug}_primary_class_histogram_{suffix}}}"
            )
        else:
            lines.append(rf"    No {label} projects were classified in this repository.")
    lines.append(
        rf"    \caption{{Primary class distributions (all, QDA, QD) --- {escape_latex(title)}}}"
    )
    lines.append(r"\end{figure}")
    return "\n".join(lines)


def render_subsection(title, slug, tables_root, res_dir):
    repo_dir = Path(tables_root) / slug
    comments_text = render_comments(repo_dir / "comments.md")

    blocks = [
        r"\clearpage",
        "",
        rf"\subsection{{{title}}}",
        "",
        comments_text,
        "",
        build_overview_figure(title, slug, res_dir),
        "",
        r"\clearpage",
        "",
        build_histograms_figure(title, slug, res_dir, repo_dir),
        "",
        r"\clearpage",
        "",
        build_table_latex(
            read_table(repo_dir / "primary_class_table.csv"),
            rf"Top primary classes --- {escape_latex(title)}",
        ),
        "",
    ]

    for suffix, type_label in SCOPED_DISTRIBUTIONS:
        label = escape_latex(type_label)
        table_path = repo_dir / f"primary_class_table_{suffix}.csv"
        if not table_path.exists():
            continue
        blocks.append(
            build_table_latex(
                read_table(table_path),
                rf"Top primary classes ({label}) --- {escape_latex(title)}",
            )
        )
        blocks.append("")

    return "\n".join(blocks)


def main():
    parser = argparse.ArgumentParser(
        description="Populate the Classification Results LaTeX section from by_repository output."
    )
    parser.add_argument(
        "--tables-root",
        default="src/phase2_classification/data_analysis/output/by_repository",
    )
    parser.add_argument("--res-dir", default="res/by_repository")
    parser.add_argument("--output", default="docs/classification_results.tex")
    args = parser.parse_args()

    sections = [INTRO]
    for title, slug in REPO_SECTIONS:
        sections.append(render_subsection(title, slug, args.tables_root, args.res_dir))

    Path(args.output).write_text("\n".join(sections))
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
