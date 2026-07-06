import argparse
import sqlite3

PLACEHOLDER_PROJECT_IDS = (27302, 27303, 3738, 3758)
FABRICATED_DOWNLOAD_DATE = "2026-04-17T00:00:00.000000+00:00"

DOWNLOAD_METHOD_TO_API_CALL = ("API", "api", "SCRAPING | API")
VALID_DOWNLOAD_METHODS = ("SCRAPING", "API-CALL")
VALID_FILE_STATUSES = ("SUCCEEDED", "FAILED_SERVER_UNRESPONSIVE", "FAILED_LOGIN_REQUIRED", "FAILED_TOO_LARGE")


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def delete_placeholder_projects(cursor):
    placeholders = ", ".join("?" for _ in PLACEHOLDER_PROJECT_IDS)
    for table in ("FILES", "KEYWORDS", "PERSON_ROLE", "LICENSES"):
        cursor.execute(f"DELETE FROM {table} WHERE project_id IN ({placeholders})", PLACEHOLDER_PROJECT_IDS)
    cursor.execute(f"DELETE FROM PROJECTS WHERE id IN ({placeholders})", PLACEHOLDER_PROJECT_IDS)
    return cursor.rowcount


def fill_project_url_from_doi(cursor):
    cursor.execute("UPDATE PROJECTS SET project_url = doi WHERE project_url = '' AND doi IS NOT NULL AND doi != ''")
    return cursor.rowcount


def normalize_download_method(cursor):
    placeholders = ", ".join("?" for _ in DOWNLOAD_METHOD_TO_API_CALL)
    cursor.execute(f"UPDATE PROJECTS SET download_method = 'API-CALL' WHERE download_method IN ({placeholders})", DOWNLOAD_METHOD_TO_API_CALL)
    to_api_call = cursor.rowcount

    valid_placeholders = ", ".join("?" for _ in VALID_DOWNLOAD_METHODS)
    cursor.execute(f"UPDATE PROJECTS SET download_method = 'SCRAPING' WHERE download_method NOT IN ({valid_placeholders})", VALID_DOWNLOAD_METHODS)
    to_scraping = cursor.rowcount

    return to_api_call, to_scraping


def normalize_file_status(cursor):
    placeholders = ", ".join("?" for _ in VALID_FILE_STATUSES)
    cursor.execute(f"UPDATE FILES SET status = 'FAILED_LOGIN_REQUIRED' WHERE status NOT IN ({placeholders})", VALID_FILE_STATUSES)
    return cursor.rowcount


def normalize_person_role(cursor):
    cursor.execute("UPDATE PERSON_ROLE SET role = 'UPLOADER' WHERE role = 'uploader'")
    return cursor.rowcount


def fabricate_missing_download_date(cursor):
    cursor.execute("UPDATE PROJECTS SET download_date = ? WHERE download_date IS NULL", (FABRICATED_DOWNLOAD_DATE,))
    return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description="Clean up known data-quality issues in the classification database.")
    parser.add_argument("--db", default="23688981-sq26-classification.db")
    args = parser.parse_args()

    conn = get_connection(args.db)
    try:
        cursor = conn.cursor()

        deleted = delete_placeholder_projects(cursor)
        print(f"Deleted {deleted} placeholder project(s): {PLACEHOLDER_PROJECT_IDS}")

        filled = fill_project_url_from_doi(cursor)
        print(f"Filled project_url from doi for {filled} project(s)")

        to_api_call, to_scraping = normalize_download_method(cursor)
        print(f"download_method: {to_api_call} row(s) -> API-CALL, {to_scraping} row(s) -> SCRAPING")

        files_fixed = normalize_file_status(cursor)
        print(f"FILES.status: {files_fixed} row(s) -> FAILED_LOGIN_REQUIRED")

        roles_fixed = normalize_person_role(cursor)
        print(f"PERSON_ROLE.role: {roles_fixed} row(s) -> UPLOADER")

        dates_fixed = fabricate_missing_download_date(cursor)
        print(f"download_date: {dates_fixed} row(s) -> {FABRICATED_DOWNLOAD_DATE}")

        conn.commit()
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
