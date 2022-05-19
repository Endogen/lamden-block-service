import json


class Config:

    _cfg = None
    _cfg_file = None

    def __init__(self, cfg_file: str):
        self._cfg_file = cfg_file
        self.load()

    def load(self):
        with open(self._cfg_file, encoding='utf-8') as f:
            self._cfg = json.load(f)

    def dump(self):
        with open(self._cfg_file, 'w', encoding='utf-8') as f:
            json.dump(self._cfg, f, ensure_ascii=False, sort_keys=True, indent=4)

    def get(self, key, load=False):
        if load: self.load()
        return self._cfg[key] if key in self._cfg else None

    def set(self, key, value, dump=True):
        self._cfg[key] = value
        if dump: self.dump()
