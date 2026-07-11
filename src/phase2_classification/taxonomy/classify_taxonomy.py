import argparse
import json
import os
import re
import sqlite3
import sys

from sentence_transformers import SentenceTransformer, util
from keybert import KeyBERT

from reference import load_sections, load_divisions, division_label

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "classification"))
from rules import PRIMARY_DATA_EXTENSIONS, JUNK_FILENAMES, normalize_extension

MODEL_NAME = "all-MiniLM-L6-v2"
PRIMARY_FILE_PROJECT_TYPES = ("QDA_PROJECT", "QD_PROJECT")


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_columns(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(PROJECTS)")
    columns = [row[1] for row in cursor.fetchall()]
    if "primary_class" not in columns:
        cursor.execute("ALTER TABLE PROJECTS ADD COLUMN primary_class TEXT")
    if "secondary_class" not in columns:
        cursor.execute("ALTER TABLE PROJECTS ADD COLUMN secondary_class TEXT")
    if "tags" not in columns:
        cursor.execute("ALTER TABLE PROJECTS ADD COLUMN tags TEXT")
    conn.commit()


def ensure_file_columns(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(FILES)")
    columns = [row[1] for row in cursor.fetchall()]
    if "primary_class" not in columns:
        cursor.execute("ALTER TABLE FILES ADD COLUMN primary_class TEXT")
    if "secondary_class" not in columns:
        cursor.execute("ALTER TABLE FILES ADD COLUMN secondary_class TEXT")
    conn.commit()


def clean_filename_text(file_name):
    base = file_name.rsplit(".", 1)[0] if "." in file_name else file_name
    return [token for token in re.split(r"[^A-Za-z]+", base) if len(token) >= 3]


def build_division_corpus():
    sections = load_sections()
    divisions = load_divisions()
    corpus = []
    for division in divisions:
        section_title = sections[division["section"]]
        text = f"{section_title}. {division['title']}"
        corpus.append({
            "section": division["section"],
            "code": division["code"],
            "title": division["title"],
            "text": text,
        })
    return corpus


def build_project_text(title, description, keywords, file_names):
    parts = [title or "", description or "", " ".join(keywords)]
    if file_names:
        parts.append(" ".join(file_names))
    return " ".join(part for part in parts if part).strip()


def classify_all(conn, model, tag_model):
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, description FROM PROJECTS")
    projects = [{"id": row[0], "title": row[1], "description": row[2]} for row in cursor.fetchall()]
    total = len(projects)

    division_corpus = build_division_corpus()
    division_embeddings = model.encode([d["text"] for d in division_corpus], convert_to_tensor=True)

    for index, project in enumerate(projects, 1):
        cursor.execute("SELECT keyword FROM KEYWORDS WHERE project_id = ?", (project["id"],))
        keywords = [row[0] for row in cursor.fetchall() if row[0]]

        cursor.execute("SELECT file_name FROM FILES WHERE project_id = ?", (project["id"],))
        file_names = [row[0] for row in cursor.fetchall() if row[0]]

        text = build_project_text(project["title"], project["description"], keywords, file_names)

        if text:
            text_embedding = model.encode(text, convert_to_tensor=True)
            scores = util.cos_sim(text_embedding, division_embeddings)[0].tolist()
            ranked = sorted(zip(division_corpus, scores), key=lambda pair: pair[1], reverse=True)
            primary_class = division_label(ranked[0][0]) if len(ranked) > 0 else None
            secondary_class = division_label(ranked[1][0]) if len(ranked) > 1 else None
            tags = [keyword for keyword, _ in tag_model.extract_keywords(text, top_n=5)]
        else:
            primary_class = None
            secondary_class = None
            tags = []

        cursor.execute(
            "UPDATE PROJECTS SET primary_class = ?, secondary_class = ?, tags = ? WHERE id = ?",
            (primary_class, secondary_class, json.dumps(tags), project["id"]),
        )

        sys.stdout.write(f"\rClassifying project {index}/{total} ({index * 100 // total}%)")
        sys.stdout.flush()

    if total:
        sys.stdout.write("\n")
    conn.commit()


def classify_files(conn, model):
    ensure_file_columns(conn)
    cursor = conn.cursor()

    division_corpus = build_division_corpus()
    division_embeddings = model.encode([d["text"] for d in division_corpus], convert_to_tensor=True)

    cursor.execute(
        """
        SELECT f.id, f.file_name, f.file_type, p.primary_class, p.secondary_class
        FROM FILES f JOIN PROJECTS p ON f.project_id = p.id
        WHERE p.type IN ('QDA_PROJECT', 'QD_PROJECT')
        """
    )
    rows = cursor.fetchall()

    to_embed = []
    fallback = []
    for file_id, file_name, file_type, project_primary, project_secondary in rows:
        name = (file_name or "").strip().lower()
        if name in JUNK_FILENAMES:
            continue
        ext = normalize_extension(file_type, file_name)
        if ext not in PRIMARY_DATA_EXTENSIONS:
            continue
        tokens = clean_filename_text(file_name)
        if len(tokens) >= 2:
            to_embed.append((file_id, " ".join(tokens)))
        else:
            fallback.append((project_primary, project_secondary, file_id))

    if fallback:
        cursor.executemany(
            "UPDATE FILES SET primary_class = ?, secondary_class = ? WHERE id = ?",
            fallback,
        )

    chunk_size = 4096
    total = len(to_embed)
    for start in range(0, total, chunk_size):
        chunk = to_embed[start:start + chunk_size]
        embeddings = model.encode(
            [text for _, text in chunk],
            convert_to_tensor=True,
            batch_size=256,
        )
        scores = util.cos_sim(embeddings, division_embeddings)
        top = scores.topk(2, dim=1)
        updates = []
        for row_index, (file_id, _) in enumerate(chunk):
            first = division_corpus[int(top.indices[row_index][0])]
            second = division_corpus[int(top.indices[row_index][1])]
            updates.append((division_label(first), division_label(second), file_id))
        cursor.executemany(
            "UPDATE FILES SET primary_class = ?, secondary_class = ? WHERE id = ?",
            updates,
        )
        done = min(start + chunk_size, total)
        sys.stdout.write(f"\rClassifying files {done}/{total} ({done * 100 // total}%)")
        sys.stdout.flush()

    if total:
        sys.stdout.write("\n")
    conn.commit()
    print(f"File classification complete: {total} by filename, {len(fallback)} by project fallback.")


def main():
    parser = argparse.ArgumentParser(description="Classify each project against the ISIC Rev.5 taxonomy and generate tags.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    parser.add_argument("--scope", choices=["projects", "files", "all"], default="all")
    args = parser.parse_args()

    print("Loading embedding model...")
    model = SentenceTransformer(MODEL_NAME)
    tag_model = KeyBERT(model=model)

    conn = get_connection(args.db)
    try:
        ensure_columns(conn)
        if args.scope in ("projects", "all"):
            classify_all(conn, model, tag_model)
        if args.scope in ("files", "all"):
            classify_files(conn, model)
    finally:
        conn.close()

    print("Taxonomy classification complete.")


if __name__ == "__main__":
    main()
