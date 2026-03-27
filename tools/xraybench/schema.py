"""JSON schema validation for benchmark specs, results, and dataset manifests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import jsonschema

    HAS_JSONSCHEMA = True
except ImportError:
    jsonschema = None  # type: ignore[assignment]
    HAS_JSONSCHEMA = False

SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"

SCHEMA_FILES = {
    "result": "result.schema.json",
    "benchmark": "benchmark.spec.schema.json",
    "dataset": "dataset.manifest.schema.json",
}


def load_schema(schema_type: str) -> dict[str, Any]:
    """Load a JSON schema by type name.

    Args:
        schema_type: One of "result", "benchmark", "dataset".

    Returns:
        Parsed JSON schema as a dictionary.

    Raises:
        ValueError: If schema_type is not recognized.
        FileNotFoundError: If the schema file does not exist.
    """
    if schema_type not in SCHEMA_FILES:
        raise ValueError(
            f"Unknown schema type '{schema_type}'. "
            f"Valid types: {', '.join(sorted(SCHEMA_FILES))}"
        )

    schema_path = SCHEMA_DIR / SCHEMA_FILES[schema_type]
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path) as f:
        return json.load(f)


def validate(data: dict[str, Any], schema_type: str) -> list[str]:
    """Validate data against a JSON schema.

    Args:
        data: The data to validate.
        schema_type: One of "result", "benchmark", "dataset".

    Returns:
        List of validation error messages. Empty list means valid.
    """
    if not HAS_JSONSCHEMA:
        return ["jsonschema package is not installed. Run: pip install jsonschema"]

    schema = load_schema(schema_type)
    validator = jsonschema.Draft7Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path))

    return [
        f"{'.'.join(str(p) for p in e.absolute_path) or '(root)'}: {e.message}"
        for e in errors
    ]


def validate_file(file_path: str | Path, schema_type: str) -> list[str]:
    """Validate a JSON or YAML file against a schema.

    Args:
        file_path: Path to the file to validate.
        schema_type: One of "result", "benchmark", "dataset".

    Returns:
        List of validation error messages. Empty list means valid.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return [f"File not found: {file_path}"]

    try:
        if file_path.suffix in (".yaml", ".yml"):
            try:
                import yaml
            except ImportError:
                return ["PyYAML is not installed. Run: pip install pyyaml"]
            with open(file_path) as f:
                data = yaml.safe_load(f)
        elif file_path.suffix == ".json":
            with open(file_path) as f:
                data = json.load(f)
        else:
            return [f"Unsupported file format: {file_path.suffix}"]
    except Exception as e:
        return [f"Failed to parse {file_path}: {e}"]

    if data is None:
        return [f"File is empty: {file_path}"]

    return validate(data, schema_type)
