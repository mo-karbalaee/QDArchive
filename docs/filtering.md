# Project Type Filtering

Code: `src/phase2_classification/classification/` · Run: `bash scripts/classify.sh`

## What it does

Assigns every project a `type` column (`PROJECT_TYPE`) describing what *kind* of research
data it actually has, based purely on the file extensions found in its `FILES` rows. No
text understanding, no metadata (title/description) involved — just extension
set-membership. This runs against the aggregated database
(`23688981-sq26-classification.db`), after Step 1 (aggregation).

`PROJECT_TYPE` is one of:

- `QDA_PROJECT` — has at least one native CAQDAS (qualitative analysis software) project file
- `QD_PROJECT` — else, has at least one primary/raw qualitative data file
- `OTHER_PROJECT` — else, has at least one recognizable, non-junk file
- `NOT_A_PROJECT` — else (no files, or only junk/unrecognized files)

## Rule order (first match wins)

For each project, every one of its files is reduced to a normalized extension
(`rules.normalize_extension`: prefer the `file_type` column, fall back to parsing the
suffix off `file_name` if that's missing). Junk filenames (`.DS_Store`, `Thumbs.db`,
`desktop.ini`) and junk extensions (`tmp`, `log`, `lock`, `ds_store`) are discarded first.
What's left is a set of extensions for the whole project, then:

1. If that set intersects `QDA_EXTENSIONS` → `QDA_PROJECT`
2. Else if it intersects `PRIMARY_DATA_EXTENSIONS` → `QD_PROJECT`
3. Else if it's non-empty at all → `OTHER_PROJECT`
4. Else → `NOT_A_PROJECT`

This means the check isn't file-by-file — it's "does this project have *any* file of this
category," evaluated across all its files at once, in priority order.

## The extension lists (`rules.py`)

- **`QDA_EXTENSIONS`** — native project files from major CAQDAS tools, plus the
  standard REFI-QDA exchange format: `qdpx`, `nvp`/`nvpx`/`nvapp` (NVivo),
  `atlproj`/`hpr7`/`hprx` (ATLAS.ti), `mx20`/`mx22`/`mx23`/`mx24` (MAXQDA), `qda`/`qdp`
  (QDA Miner), `tra` (Transana), `f4x`/`f4p` (f4analyse/f4transkript).
- **`PRIMARY_DATA_EXTENSIONS`** — raw/source qualitative data collected directly from
  participants: transcripts (`docx`, `doc`, `txt`, `pdf`, `rtf`) and audio/video
  recordings (`mp3`, `wav`, `m4a`, `aac`, `flac`, `ogg`, `wma`, `mp4`, `mov`, `avi`, `mkv`,
  `wmv`, `webm`).
- **`JUNK_FILENAMES` / `JUNK_EXTENSIONS`** — OS/system clutter that shouldn't count as a
  "valid" file at all, so it doesn't push a project into `OTHER_PROJECT` on its own.

These three lists were agreed on with the user up front rather than assumed, since
CLAUDE.md explicitly flags them as open definitions.

## Running it (`classify.py`)

1. Adds the `type` column to `PROJECTS` if it isn't already there (`ensure_type_column`).
2. Loads every project ID, then for each one loads its `FILES` rows and calls
   `classify_project`.
3. Writes the resulting `PROJECT_TYPE` back onto that row, tallies it, and prints a live
   `Classifying project N/Total (X%)` progress line (overwritten in place via `\r`).
4. At the end, prints the tally of how many projects landed in each of the four buckets.
