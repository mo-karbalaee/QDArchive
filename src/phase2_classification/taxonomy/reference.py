import json
from pathlib import Path

_TAXONOMY_PATH = Path(__file__).parent / "isic_rev5.json"


def load_taxonomy():
    with open(_TAXONOMY_PATH) as f:
        return json.load(f)


def load_sections():
    return load_taxonomy()["sections"]


def load_divisions():
    return load_taxonomy()["divisions"]


def division_label(division):
    return f"{division['section']}{division['code']} - {division['code']} - {division['title']}"
