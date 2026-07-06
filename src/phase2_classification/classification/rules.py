import json
import os

_EXTENSIONS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "qda_file_extensions.json")


def _load_qda_extensions(path=_EXTENSIONS_PATH):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {ext.strip().lower().lstrip(".") for exts in data.values() for ext in exts}


QDA_EXTENSIONS = _load_qda_extensions()

PRIMARY_DATA_EXTENSIONS = {
    "docx", "doc", "txt", "pdf", "rtf",
    "mp3", "wav", "m4a", "aac", "flac", "ogg", "wma",
    "mp4", "mov", "avi", "mkv", "wmv", "webm",
}

JUNK_FILENAMES = {".ds_store", "thumbs.db", "desktop.ini"}
JUNK_EXTENSIONS = {"tmp", "log", "lock", "ds_store"}


def normalize_extension(file_type, file_name):
    candidate = (file_type or "").strip().lower().lstrip(".")
    if candidate:
        return candidate
    if file_name and "." in file_name:
        return file_name.strip().lower().rsplit(".", 1)[-1]
    return ""


def classify_project(files):
    extensions = set()
    for f in files:
        name = (f.get("file_name") or "").strip().lower()
        if name in JUNK_FILENAMES:
            continue
        ext = normalize_extension(f.get("file_type"), f.get("file_name"))
        if not ext or ext in JUNK_EXTENSIONS:
            continue
        extensions.add(ext)

    if extensions & QDA_EXTENSIONS:
        return "QDA_PROJECT"
    if extensions & PRIMARY_DATA_EXTENSIONS:
        return "QD_PROJECT"
    if extensions:
        return "OTHER_PROJECT"
    return "NOT_A_PROJECT"
