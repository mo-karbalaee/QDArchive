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

In this section, I present the results of the classification procedures applied to the aggregated database. Every project was first assigned a project type --- \texttt{QDA\_PROJECT}, \texttt{QD\_PROJECT}, \texttt{OTHER\_PROJECT}, or \texttt{NOT\_A\_PROJECT} --- derived from the file extensions present in the project. Projects were then classified against the ISIC Rev.~5 taxonomy at the division level (two levels deep, i.e.\ section and then division) using their title, description, keywords, and file names. This yields a primary and, where applicable, a secondary division for each project, together with a set of free-form tags that support search and discovery.

The analysis is organised on a per-repository basis so that patterns can be compared across sources and interpreted in the context of each repository's size and composition. In line with the classification requirements, the primary-class distributions are reported both for all projects in a repository and, separately, restricted to the \texttt{QDA\_PROJECT} and \texttt{QD\_PROJECT} types, as these are the project types of primary interest.

Each repository subsection is organised as follows:

\begin{enumerate}
    \item \textbf{Overview.} A short narrative summary of the repository, followed by its key facts (total number of projects, the number of distinct primary classes observed, and the dominant class) and its composition by project type. Two charts accompany this summary: a pie chart of the primary-class distribution across all projects, and a donut chart of the project-type composition.

    \item \textbf{Primary-class histograms.} Histograms of the primary classes detected in the repository, shown for all projects and, separately, for the QDA and QD project types. Each bar is annotated with its count, the full ISIC division name is used as the label, and the graphics are rendered as vector images so that they remain legible when magnified.

    \item \textbf{Rank-ordered class tables.} For the same three groupings (all projects, QDA projects, and QD projects), a table listing the twenty most frequent primary classes together with their counts.

    \item \textbf{File-level classification.} For the QDA and QD projects, each primary data file --- documents and audio or video material such as PDF, DOCX, TXT, RTF, and common media formats --- was additionally classified against the same taxonomy. Each file is classified from its file name, falling back to the classification of its parent project when the file name is uninformative. These results are reported as a histogram of the primary classes assigned to the repository's primary data files, accompanied by a ranked table of the ten most frequent classes on the same page.
\end{enumerate}

Unless stated otherwise, all percentages are computed within the grouping to which they refer.
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
            lines.append("")
            lines.append(rf"\textbf{{{escape_latex(stripped[3:])}}}")
            lines.append("")
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
        lines.append("")
        lines.append(escape_latex(stripped))
        lines.append("")

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

    file_histogram = repo_dir / f"{slug}_file_primary_class_histogram.svg"
    file_table = repo_dir / "file_primary_class_table.csv"
    if file_histogram.exists() or file_table.exists():
        blocks.append(r"\clearpage")
        blocks.append("")
        if file_histogram.exists():
            blocks.append(
                build_figure_latex(
                    res_dir, slug, f"{slug}_file_primary_class_histogram",
                    rf"Primary class distribution across primary data files --- {escape_latex(title)}",
                )
            )
            blocks.append("")
        if file_table.exists():
            blocks.append(
                build_table_latex(
                    read_table(file_table),
                    rf"Top primary classes, primary data files --- {escape_latex(title)}",
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
