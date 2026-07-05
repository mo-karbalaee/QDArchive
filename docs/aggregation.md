# Database Aggregation

Code: `src/phase2_classification/aggregation/` · Run: `bash scripts/aggregate.sh`

## Problem

`databases/` holds 39 SQLite databases built independently by different students for the
same class assignment. All of them describe the same five entities (`projects`, `files`,
`keywords`, `licenses`, `person_role`), but inconsistently:

- table/column casing varies (`PROJECTS` vs `projects`)
- some databases are missing columns our canonical schema expects
- some databases add extra columns/tables from their own attempts at Phase 2/3
  (`isic_section`, `type`, `tags`, `REPOSITORIES`, ...)

Aggregation merges all 39 into one deduplicated database using our own Phase 1 schema
(`src/phase1_acquisition/database.py`) as the canonical target.

## Pipeline (`aggregate.py`)

1. Read every `*.db` file in `databases/`, sorted by filename for a deterministic order.
2. For each source database, resolve its actual table/column names case-insensitively
   against the canonical schema (`mapping.py`). Missing canonical columns become `NULL`;
   non-canonical extra columns/tables (e.g. other students' classification attempts) are
   dropped entirely — they're not authoritative and we're going to (re)classify ourselves.
3. Feed every `projects` row into the `Aggregator` (`merge.py`), which decides whether it's
   a duplicate of a project already seen, or a new project.
4. Feed every `files` / `keywords` / `person_role` / `licenses` row into the merged project
   it belongs to (looked up via the source database's local `project_id`).
5. Write the merged result out to a fresh database with the canonical schema
   (`schema.py`).

## Deduplication key (fallback chain)

A project is considered a duplicate of an existing one if any of these match, checked in
order:

1. **`project_url`** (normalized: trimmed, trailing slash removed, lowercased)
2. **`doi`** (normalized: trimmed, lowercased) — used only if there's no URL match
3. **`title` + `repository_id`** (title normalized: whitespace-collapsed, lowercased) —
   used only if neither of the above match

The first matching tier wins; later tiers are only consulted if the earlier ones don't
apply (e.g. the field is `NULL` on one side).

## Merge policy

When a project is matched as a duplicate:

- **Scalar fields** (title, description, doi, ...): coalesced — the first non-`NULL` value
  seen for each field is kept; a later duplicate can *fill in* a field that was still
  `NULL`, but never overwrites a value that's already set.
- **Child records** (files, keywords, person_role, licenses): unioned, not just
  concatenated — each duplicate is deduplicated within a merged project:
  - files: by normalized `file_name`
  - keywords: by normalized text
  - person_role: by normalized `(name, role)` pair
  - licenses: by normalized text
  
  So the same file appearing under different casing across two source databases (e.g.
  `interview1.docx` vs `INTERVIEW1.docx`) is stored once, not twice.

## What's deliberately out of scope

- No content is read from the databases during development/testing of this code — only
  schema (table/column names) was inspected to design the mapping logic; the merge logic
  itself was validated against synthetic fixture databases, not the real ones.
- Other students' own classification attempts (extra columns/tables) are dropped, not
  preserved, since Phase 2 Steps 2 and 3 redo this classification from scratch.
