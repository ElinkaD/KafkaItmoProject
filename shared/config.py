import os
from pathlib import Path


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Environment variable {name} must be set")
    return value


def require_int_env(name: str) -> int:
    value = require_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer, got {value!r}") from exc


def require_float_env(name: str) -> float:
    value = require_env(name)
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be a float, got {value!r}") from exc


def require_path_env(name: str) -> Path:
    return Path(require_env(name))
