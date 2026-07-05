import argparse
import sqlite3
from pathlib import Path

from schema import CANONICAL_ENTITIES, get_cursor, init_output_db
from mapping import list_source_tables, resolve_table, source_columns, build_select
from merge import Aggregator


def rows_as_dicts(cursor):
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def ingest_database(db_path, aggregator):
    with get_cursor(db_path) as cursor:
        source_tables = list_source_tables(cursor)

        projects_table = resolve_table(source_tables, "projects")
        if projects_table is None:
            print(f"  skipping {db_path.name}: no projects table found")
            return 0

        projects_columns = source_columns(cursor, projects_table)
        projects_select = build_select(
            projects_table, projects_columns, CANONICAL_ENTITIES["projects"]["columns"]
        )
        cursor.execute(projects_select)
        project_rows = rows_as_dicts(cursor)

        local_id_to_merged = {}
        for row in project_rows:
            local_id = row.pop("id")
            merged_index = aggregator.upsert_project(row)
            local_id_to_merged[local_id] = merged_index

        for entity_name, add_fn in (
            ("files", aggregator.add_file),
            ("keywords", None),
            ("person_role", None),
            ("licenses", None),
        ):
            table_name = resolve_table(source_tables, entity_name)
            if table_name is None:
                continue

            entity_spec = CANONICAL_ENTITIES[entity_name]
            columns = source_columns(cursor, table_name)
            select = build_select(
                table_name, columns, entity_spec["columns"],
                parent_column=entity_spec["parent_column"],
            )
            if select is None:
                continue

            cursor.execute(select)
            for row in rows_as_dicts(cursor):
                merged_index = local_id_to_merged.get(row.get("project_id"))
                if merged_index is None:
                    continue

                if entity_name == "files":
                    aggregator.add_file(merged_index, {
                        "file_name": row.get("file_name"),
                        "file_type": row.get("file_type"),
                        "status": row.get("status"),
                    })
                elif entity_name == "keywords":
                    aggregator.add_keyword(merged_index, row.get("keyword"))
                elif entity_name == "person_role":
                    aggregator.add_person_role(merged_index, row.get("name"), row.get("role"))
                elif entity_name == "licenses":
                    aggregator.add_license(merged_index, row.get("license"))

        return len(project_rows)


def write_output(aggregator, output_path):
    init_output_db(output_path)
    with get_cursor(output_path) as cursor:
        for merged in aggregator.projects:
            fields = merged.fields
            cursor.execute('''
                INSERT INTO PROJECTS (
                    query_string, repository_id, repository_url, project_url,
                    version, title, description, language, doi, upload_date,
                    download_date, download_repository_folder, download_project_folder,
                    download_version_folder, download_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fields.get("query_string"), fields.get("repository_id"),
                fields.get("repository_url"), fields.get("project_url"),
                fields.get("version"), fields.get("title"), fields.get("description"),
                fields.get("language"), fields.get("doi"), fields.get("upload_date"),
                fields.get("download_date"), fields.get("download_repository_folder"),
                fields.get("download_project_folder"), fields.get("download_version_folder"),
                fields.get("download_method"),
            ))
            project_id = cursor.lastrowid

            if merged.files:
                cursor.executemany(
                    "INSERT INTO FILES (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
                    [(project_id, f["file_name"], f.get("file_type"), f.get("status")) for f in merged.files.values()]
                )
            if merged.keywords:
                cursor.executemany(
                    "INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)",
                    [(project_id, kw) for kw in merged.keywords.values()]
                )
            if merged.person_role:
                cursor.executemany(
                    "INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)",
                    [(project_id, name, role) for name, role in merged.person_role.values()]
                )
            if merged.licenses:
                cursor.executemany(
                    "INSERT INTO LICENSES (project_id, license) VALUES (?, ?)",
                    [(project_id, lic) for lic in merged.licenses.values()]
                )


def main():
    parser = argparse.ArgumentParser(description="Aggregate and deduplicate source databases into one database.")
    parser.add_argument("--source-dir", default="databases")
    parser.add_argument("--output", default="aggregated.db")
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    output_path = Path(args.output)

    db_files = sorted(source_dir.glob("*.db"))
    aggregator = Aggregator()

    total_input_rows = 0
    for db_path in db_files:
        print(f"Processing {db_path.name}...")
        total_input_rows += ingest_database(db_path, aggregator)

    write_output(aggregator, output_path)

    print(f"\nSource databases processed: {len(db_files)}")
    print(f"Total project rows read: {total_input_rows}")
    print(f"Deduplicated project count: {len(aggregator.projects)}")
    print(f"Output written to: {output_path}")


if __name__ == "__main__":
    main()
