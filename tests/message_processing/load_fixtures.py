import json
import os
from typing import Dict, List
from pathlib import Path


def load_fixture_messages(fixture: str) -> List[Dict]:
    fixture_path = Path(__file__).parent / "fixtures" / fixture

    with open(fixture_path) as f:
        return json.load(f)["content"]["messages"]
