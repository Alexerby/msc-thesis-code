import json
from pathlib import Path

def load_config(config_path=None):
    # Default path two levels up from this file (adjust if needed)
    if config_path is None:
        this_dir = Path(__file__).resolve().parent
        config_path = this_dir.parent / 'config' / 'config.json'
    else:
        config_path = Path(config_path).expanduser().resolve()
    with open(config_path, 'r') as f:
        return json.load(f)
