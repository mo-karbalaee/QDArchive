from schema import CANONICAL_ENTITIES


def list_source_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [row[0] for row in cursor.fetchall()]


def resolve_table(source_tables, canonical_name):
    for name in source_tables:
        if name.lower() == canonical_name:
            return name
    return None


def source_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    return [row[1] for row in cursor.fetchall()]


def resolve_column(columns, canonical_column):
    for name in columns:
        if name.lower() == canonical_column:
            return name
    return None


def build_select(table_name, columns, canonical_columns, id_column="id", parent_column=None):
    """Builds a SELECT statement mapping canonical columns onto whatever the
    source actually has, using NULL for columns the source is missing."""
    select_parts = [f'"{id_column}" AS id']
    if parent_column:
        actual_parent = resolve_column(columns, parent_column)
        if actual_parent is None:
            return None
        select_parts.append(f'"{actual_parent}" AS {parent_column}')

    for canonical_column in canonical_columns:
        actual_column = resolve_column(columns, canonical_column)
        if actual_column is None:
            select_parts.append(f'NULL AS {canonical_column}')
        else:
            select_parts.append(f'"{actual_column}" AS {canonical_column}')

    return f'SELECT {", ".join(select_parts)} FROM "{table_name}"'


def normalize_url(value):
    if not value:
        return None
    return value.strip().rstrip('/').lower() or None


def normalize_doi(value):
    if not value:
        return None
    return value.strip().lower() or None


def normalize_title(value):
    if not value:
        return None
    normalized = ' '.join(value.split()).lower()
    return normalized or None


def normalize_text(value):
    if value is None:
        return None
    normalized = ' '.join(str(value).split()).lower()
    return normalized or None
