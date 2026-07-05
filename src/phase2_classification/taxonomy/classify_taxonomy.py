import argparse
import json
import sqlite3
import sys

from sentence_transformers import SentenceTransformer, util
from keybert import KeyBERT

from reference import load_sections, load_divisions, division_label

MODEL_NAME = "all-MiniLM-L6-v2"


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


def main():
    parser = argparse.ArgumentParser(description="Classify each project against the ISIC Rev.5 taxonomy and generate tags.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    args = parser.parse_args()

    print("Loading embedding model...")
    model = SentenceTransformer(MODEL_NAME)
    tag_model = KeyBERT(model=model)

    conn = get_connection(args.db)
    try:
        ensure_columns(conn)
        classify_all(conn, model, tag_model)
    finally:
        conn.close()

    print("Taxonomy classification complete.")


if __name__ == "__main__":
    main()
