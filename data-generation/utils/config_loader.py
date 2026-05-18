import yaml
from pathlib import Path

def load_config():
    config_path = Path(__file__).resolve().parents[2] / "configs" / "config.yaml"

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config