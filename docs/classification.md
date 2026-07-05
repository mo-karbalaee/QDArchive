# Taxonomy Classification

Code: `src/phase2_classification/taxonomy/` · Run: `bash scripts/classify_taxonomy.sh`

## What it does

Assigns every project a **subject/industry classification** against the official
[ISIC Rev. 5](https://unstats.un.org/unsd/classifications/Econ/) taxonomy (Section →
Division, e.g. `Q85 - 85 - Education`), plus a handful of freeform search tags. This is a
different question from `filtering.md`'s `PROJECT_TYPE`: that asks "what kind of data does
this project have," this asks "what field of study is this project about."

## The taxonomy reference (`isic_rev5.json`, `reference.py`)

The official ISIC Rev. 5 structure (22 Sections, 87 Divisions) was downloaded directly
from the UN Stats structure CSV linked in CLAUDE.md and saved as
`taxonomy/isic_rev5.json` — `{"sections": {letter: title}, "divisions": [{section, code,
title}, ...]}`. `reference.py` provides small loaders (`load_sections`, `load_divisions`)
and `division_label()`, which formats a division as `"{section}{code} - {code} - {title}"`
(e.g. `"Q85 - 85 - Education"`).

## Method: local semantic similarity, no API calls

There's no fixed keyword-to-division mapping and no external LLM call. Instead:

1. Every one of the 87 divisions is turned into a short text —
   `"{section title}. {division title}"` — and embedded once with a local
   `sentence-transformers` model (`all-MiniLM-L6-v2`, already installed as a dependency of
   `keybert`). This is the fixed "division corpus."
2. Each project is turned into its own text by concatenating `title`, `description`,
   `keywords`, and the project's file names (`build_project_text`) — file names are a weak
   secondary signal, since actual file *content* isn't available for the 38 external
   student databases, only their metadata.
3. The project's text is embedded with the same model, then compared against all 87
   division embeddings via cosine similarity (`sentence_transformers.util.cos_sim`).
4. Divisions are ranked by similarity score; the **top-1** match becomes `primary_class`
   and the **top-2** match becomes `secondary_class` (both stored as the formatted label
   string, e.g. `"R86 - 86 - Human health activities"`). If a project has no usable text at
   all, both columns are left `NULL`.

This is deterministic given the model (no training, no labeled data needed) and fully
offline/free — no API key or per-call cost, which matters given classification runs over
hundreds of merged projects.

## Tags

Generated with `KeyBERT`, reusing the *same* embedding model instance
(`KeyBERT(model=model)`), applied to the same per-project text used for classification:

1. KeyBERT extracts candidate words/short phrases directly from the project's own text
   (via a `CountVectorizer` — there's no fixed tag vocabulary).
2. It embeds every candidate and the whole document, then ranks candidates by cosine
   similarity to the document embedding.
3. The top 5 are kept as `tags`.

Since SQLite has no array type, `tags` is stored as a JSON-encoded string
(`json.dumps([...])`) in a `TEXT` column — decode with `json.loads()` when reading it back.

## Columns added to `PROJECTS`

| Column | Type | Meaning |
|---|---|---|
| `primary_class` | `TEXT` | Best-matching `"{section}{code} - {code} - {title}"` label |
| `secondary_class` | `TEXT` | Second-best-matching label |
| `tags` | `TEXT` (JSON array) | Top 5 freeform keywords/phrases |

These are independent of `filtering.md`'s `type` column — running one classifier never
touches the other's columns.
