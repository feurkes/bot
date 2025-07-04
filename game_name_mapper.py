import json
import os
from threading import Lock

def _default_mapping():
    return {
        "CS:GO": "Counter-Strike: GO",
        "Counter-Strike GO": "Counter-Strike: GO",
        "Counter-Strike:GO": "Counter-Strike: GO",
        "Counter Strike GO": "Counter-Strike: GO",
        "Counter Strike: GO": "Counter-Strike: GO",
        "Dota2": "Dota 2",
        "Dota 2": "Dota 2",
        "DOTA2": "Dota 2",
    }

class GameNameMapper:
    _mapping_file = os.path.join(os.path.dirname(__file__), 'game_name_mapping.json')
    _lock = Lock()

    def __init__(self):
        self.mapping = _default_mapping()
        self._load()

    def _load(self):
        if os.path.exists(self._mapping_file):
            try:
                with open(self._mapping_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.mapping.update(data)
            except Exception:
                pass

    def _save(self):
        with self._lock:
            try:
                with open(self._mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(self.mapping, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

    def normalize(self, game_name: str) -> str:
        name = game_name.strip()
        return self.mapping.get(name, name)

    def add_game(self, raw_name: str, normalized_name: str = None):
        name = raw_name.strip()
        normalized = normalized_name.strip() if normalized_name else name
        if name not in self.mapping:
            self.mapping[name] = normalized
            self._save()

mapper = GameNameMapper()
