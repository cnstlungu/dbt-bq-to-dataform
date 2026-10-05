"""Regression tests for edge cases a review found, each on a small project built here."""

import json
from pathlib import Path

import pytest

from dbt_bq_to_dataform import seeds as seedlib
from dbt_bq_to_dataform.converter import Converter, Options, write_output
from dbt_bq_to_dataform.dbt_project import load_project

from test_convert import EDGE, compile_dataform, issues

PROJECT = {
    "dbt_project.yml": """
name: review
profile: edge
version: "1.0"
config-version: 2
vars:
  statuses: ["a", "b"]
  min_amount: 5
seeds:
  review:
    piped:
      +delimiter: "|"
""",
    "models/target_info.sql": """
{{ config(schema='staging') }}
SELECT '{{ target.name }}' AS t, '{{ target.schema }}' AS s, '{{ target.project }}' AS p
""",
    "models/orders.sql": "SELECT 1 AS id, 'a' AS status, 10 AS amount\n",
    "models/contracted.sql": "SELECT 1 AS id\n",
    "models/literal_dollar.sql": """
{{ config(materialized='incremental', unique_key='id') }}
SELECT 1 AS id, '${not_js}' AS literal
{% if is_incremental() %}
WHERE '${also_not_js}' IS NOT NULL
{% endif %}
""",
    "models/run_started_at_plain.sql": "SELECT TIMESTAMP('{{ run_started_at }}') AS now_ts\n",
    "models/run_started_at_derived.sql": (
        "SELECT TIMESTAMP('{{ run_started_at + modules.datetime.timedelta(days=1) }}') AS tomorrow\n"
    ),
    "models/run_started_at_replaced.sql": "SELECT TIMESTAMP('{{ run_started_at.replace(hour=0) }}') AS midnight\n",
    "models/uses_both_t.sql": "SELECT * FROM {{ source('a', 't') }} UNION ALL SELECT * FROM {{ source('b', 't') }}\n",
    "models/sources.yml": """
sources:
  - name: a
    database: proj-a
    schema: raw
    tables: [{name: t}]
  - name: b
    database: proj-b
    schema: raw
    tables: [{name: t}]
""",
    "models/schema.yml": """
models:
  - name: orders
    columns:
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: "{{ var('statuses') }}"
      - name: amount
        data_tests:
          - accepted_values:
              arguments:
                values: ["{{ var('min_amount') }}", 10]
                quote: false
          - labelled:
              arguments:
                label: "{{ var('min_amount') }} to {{ var('min_amount') }}"
  - name: contracted
    config:
      contract: {enforced: true}
    columns:
      - name: id
        data_type: int64
        constraints:
          - type: not_null
  - name: piped
""",
    "tests/generic/labelled.sql": """
{% test labelled(model, column_name, label) %}
SELECT '{{ label }}' AS label FROM {{ model }} WHERE {{ column_name }} IS NULL
{% endtest %}
""",
    "seeds/multiline.csv": 'id,note\n1,"line one\nline two"\n',
    "seeds/wide.csv": "note,id\n" + "".join(f"{'x' * (2000 if i == 1 else 1)},{i}\n" for i in range(1, 301)),
    "seeds/piped.csv": "id|name\n1|a\n2|b\n",
    "seeds/schema.yml": """
seeds:
  - name: piped
    columns:
      - name: id
        data_tests: [not_null]
""",
}


def build(root: Path, files: dict[str, str]) -> Path:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content.lstrip("\n"))
    return root


def convert(project_dir: Path, **options):
    project = load_project(project_dir, profiles_dir=EDGE)
    try:
        converter = Converter(project, Options(**options))
        return converter, converter.convert()
    finally:
        project.close()


@pytest.fixture(scope="module")
def review(tmp_path_factory):
    return convert(build(tmp_path_factory.mktemp("review"), PROJECT))


def test_target_is_the_profile_target_not_the_model(review):
    _, files = review
    sql = files["definitions/target_info.sqlx"]
    assert "'dev' AS t, 'analytics' AS s, 'edge-project' AS p" in sql


def test_inline_assertions_get_rendered_arguments(review):
    _, files = review
    orders = files["definitions/orders.sqlx"]
    assert "status IN ('a', 'b')" in orders
    assert "{{" not in orders
    # An argument that renders to a Dataform var cannot go in a config string.
    standalone = [p for p in files if p.startswith("definitions/assertions/generic/accepted_values_orders_amount")]
    assert standalone and "${dataform.projectConfig.vars.min_amount}" in files[standalone[0]]


def test_not_null_constraints_become_assertions(review):
    converter, files = review
    assert 'nonNull: ["id"]' in files["definitions/contracted.sqlx"]


def test_multiline_csv_values_keep_their_newlines(review):
    _, files = review
    assert r"'line one\nline two'" in files["definitions/seeds/multiline.sqlx"]


def test_inline_size_is_the_generated_sql(review):
    _, files = review
    wide = files["definitions/seeds/wide.sqlx"]
    assert 'type: "table"' in wide
    assert len(wide) < 20_000  # one long value no longer pads every row


def test_literal_dollar_brace_is_not_javascript(review, tmp_path):
    _, files = review
    sql = files["definitions/literal_dollar.sqlx"]
    assert "'${\"${\"}not_js}'" in sql
    assert "${\"${\"}also_not_js}" in sql
    compiled = compile_dataform(files, tmp_path / "out")
    table = next(t for t in compiled["tables"] if t["target"]["name"] == "literal_dollar")
    assert "'${not_js}'" in table["query"]
    assert "'${also_not_js}'" in table["incrementalQuery"]


def test_values_derived_from_run_started_at_are_not_passed_off_as_now(review):
    converter, files = review
    assert "${new Date().toISOString()}" in files["definitions/run_started_at_plain.sqlx"]
    manual = issues(converter, "manual")
    for model in ("run_started_at_derived", "run_started_at_replaced"):
        assert "disabled: true" in files[f"definitions/{model}.sqlx"]
        assert "run_started_at" in manual[model][0]


def test_same_table_name_in_two_projects(review, tmp_path):
    _, files = review
    sql = files["definitions/uses_both_t.sqlx"]
    assert 'ref({database: "proj-a", schema: "raw", name: "t"})' in sql
    assert 'ref({database: "proj-b", schema: "raw", name: "t"})' in sql


def test_test_arguments_with_several_expressions(review):
    _, files = review
    labelled = [p for p in files if "/labelled_orders_amount" in p]
    assert labelled, sorted(files)
    assert "'${dataform.projectConfig.vars.min_amount} to ${dataform.projectConfig.vars.min_amount}'" in files[labelled[0]]


def test_large_seeds_keep_their_tests_and_delimiter(tmp_path, monkeypatch):
    monkeypatch.setattr(seedlib, "INLINE_MAX_BYTES", 10)
    converter, files = convert(build(tmp_path / "p", PROJECT))
    piped = files["definitions/seeds/piped.sqlx"]
    assert 'type: "operations"' in piped and "field_delimiter = '|'" in piped
    not_null = [p for p in files if p.startswith("definitions/assertions/generic/not_null_piped_id")]
    assert not_null, sorted(files)


def test_regeneration_never_overwrites_files_it_did_not_write(tmp_path):
    out = tmp_path / "out"
    write_output({"definitions/a.sqlx": "SELECT 1"}, out, force=False)
    (out / "definitions" / "b.sqlx").write_text("-- written by hand")
    with pytest.raises(FileExistsError, match="b.sqlx"):
        write_output({"definitions/a.sqlx": "SELECT 1", "definitions/b.sqlx": "SELECT 2"}, out, force=False)
    assert (out / "definitions" / "b.sqlx").read_text() == "-- written by hand"
    with pytest.raises(FileExistsError, match="b.sqlx"):
        write_output({"definitions/b.sqlx": "SELECT 2"}, out, force=True)
    assert json.loads((out / ".dbt-bq-to-dataform.json").read_text())["files"] == ["definitions/a.sqlx"]
