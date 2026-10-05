"""dbt seeds as Dataform tables.

Dataform has no seeds. A small CSV becomes a table built from an inline
typed array literal, which keeps the data in the repository the way a dbt
seed does. A large one would blow past BigQuery's query length limit, so it
becomes a LOAD DATA operation over a copy of the CSV the user uploads to GCS.

Column types are the ones dbt-bigquery would load: dbt's own CSV inference
when dbt is importable here, and the same rules written out otherwise.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

_INT = re.compile(r"^-?\d+$")
_NUMBER = re.compile(r"^-?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")
_NULLS = ("", "null")

# Rough BigQuery ceiling is 1 MB of unresolved SQL; stay well under it.
INLINE_MAX_BYTES = 400_000


@dataclass
class SeedTable:
    columns: list[str]
    types: list[str]  # BigQuery types, as dbt-bigquery would create them
    text_columns: set[int]  # forced to text by column_types: "null" stays a string
    rows: list[list[str]]
    size_bytes: int
    inferred_by_dbt: bool


def read_seed(path: Path, column_types: dict[str, str], delimiter: str = ",") -> SeedTable:
    text = path.read_text(encoding="utf-8-sig")
    reader = csv.reader(text.splitlines(), delimiter=delimiter)
    header = next(reader)
    rows = [r for r in reader if r]
    declared = {k.lower(): v for k, v in (column_types or {}).items()}
    inferred = _dbt_inferred_types(path, list(column_types or {}), delimiter)
    types = []
    for i, name in enumerate(header):
        given = declared.get(name.lower())
        if given:
            types.append(str(given).upper())
        elif inferred is not None:
            types.append(inferred[i].upper())
        else:
            types.append(infer_type([r[i] if i < len(r) else "" for r in rows]))
    return SeedTable(
        columns=header,
        types=types,
        text_columns={i for i, name in enumerate(header) if name.lower() in declared},
        rows=rows,
        size_bytes=len(text.encode()),
        inferred_by_dbt=inferred is not None,
    )


def _dbt_inferred_types(path: Path, text_columns: list[str], delimiter: str) -> list[str] | None:
    """dbt-bigquery's own inference, when dbt-bigquery is importable here."""
    try:
        from dbt.adapters.bigquery.impl import BigQueryAdapter
        from dbt_common.clients import agate_helper
    except Exception:
        return None
    table = agate_helper.from_csv(str(path), text_columns, delimiter=delimiter)
    return [BigQueryAdapter.convert_type(table, i) or "string" for i in range(len(table.column_names))]


def infer_type(values: list[str]) -> str:
    """dbt's CSV inference (agate_helper.build_type_tester), in BigQuery terms.

    The first type every value fits wins, in dbt's order, so a column with no
    values at all is INT64, as dbt-bigquery creates it.
    """
    present = [v.strip() for v in values if v.strip().lower() not in _NULLS]
    if all(_INT.match(v) for v in present):
        return "INT64"
    if all(_NUMBER.match(v) for v in present):
        return "FLOAT64" if any("." in v or "e" in v.lower() for v in present) else "INT64"
    if all(_DATE.match(v) for v in present):
        return "DATE"
    if all(_DATETIME.match(v) or _ISO_DATETIME.match(v) for v in present):
        return "DATETIME"
    if all(v in ("true", "false") for v in present):
        return "BOOL"
    return "STRING"


def _literal(value: str, bq_type: str, text: bool) -> str:
    if value == "" or (not text and value.strip().lower() == "null"):
        return "NULL"
    base = bq_type.split("(")[0].split("<")[0].upper()
    if base in ("INT64", "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return str(int(value.strip()))
    if base == "FLOAT64":
        return value.strip()
    if base in ("NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"):
        return f"{base} '{value.strip()}'"
    if base in ("BOOL", "BOOLEAN"):
        return "TRUE" if value.strip().lower() == "true" else "FALSE"
    if base in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
        return f"{base} '{value.strip()}'"
    return _string(value)


def _string(value: str) -> str:
    if "'" not in value and "\\" not in value and "\n" not in value:
        return f"'{value}'"
    if '"' not in value and "\\" not in value and "\n" not in value:
        return f'"{value}"'
    escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")
    return f"'{escaped}'"


def _ident(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else f"`{name}`"


def inline_sql(seed: SeedTable) -> str:
    struct = ", ".join(f"{_ident(c)} {t}" for c, t in zip(seed.columns, seed.types))
    if not seed.rows:
        return f"SELECT *\nFROM UNNEST(ARRAY<STRUCT<{struct}>>[])\n"
    widths = [0] * len(seed.columns)
    rendered = []
    for row in seed.rows:
        cells = [
            _literal(row[i] if i < len(row) else "", t, i in seed.text_columns)
            for i, t in enumerate(seed.types)
        ]
        rendered.append(cells)
        widths = [max(w, len(c)) for w, c in zip(widths, cells)]
    lines = []
    for n, cells in enumerate(rendered):
        padded = " ".join(
            (c + ",").ljust(widths[i] + 1) if i < len(cells) - 1 else c for i, c in enumerate(cells)
        )
        comma = "," if n < len(rendered) - 1 else ""
        lines.append(f"  ({padded}){comma}")
    body = "\n".join(lines)
    return f"SELECT *\nFROM UNNEST(ARRAY<STRUCT<{struct}>>[\n{body}\n])\n"


def load_data_sql(seed: SeedTable, uri_js: str) -> str:
    schema = ",\n  ".join(f"{_ident(c)} {t}" for c, t in zip(seed.columns, seed.types))
    return (
        "LOAD DATA OVERWRITE ${self()} (\n"
        f"  {schema}\n"
        ")\n"
        "FROM FILES (\n"
        "  format = 'CSV',\n"
        "  skip_leading_rows = 1,\n"
        f"  uris = ['{uri_js}']\n"
        ")\n"
    )
