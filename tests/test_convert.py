"""End-to-end: convert dbt-bigquery projects, then compile the result with Dataform.

The edge fixture (tests/fixtures/edge_project, with its local package
edge_pkg) exercises what real projects lean on: dispatch, package macros,
ref() overrides, scoped vars, tests with thresholds, seeds with hooks.

The golden example is dbt-labs/jaffle-shop, parsed as a BigQuery project. It
is read from the checkout DBT2DATAFORM_EXAMPLE names (default ../jaffle-shop)
and skipped when absent, unless DBT2DATAFORM_REQUIRE_EXAMPLE is set (CI sets
it). Compiling needs `npx`; set DBT2DATAFORM_SKIP_COMPILE=1 to skip that.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from dbt2dataform.cli import main
from dbt2dataform.converter import Converter, Options, write_output
from dbt2dataform.dbt_project import ProjectError, load_project

HERE = Path(__file__).parent
EDGE = HERE / "fixtures" / "edge_project"
PROFILES = HERE / "fixtures" / "profiles"
JAFFLE = Path(os.environ.get("DBT2DATAFORM_EXAMPLE") or HERE.parent.parent / "jaffle-shop")
JAFFLE_COMMIT = "5beb145b00f5465ec759cfcdd9745e858818cf95"
EXAMPLE = HERE.parent / "examples" / "jaffle_shop"
DATAFORM_CLI = "@dataform/cli@3.0.71"

needs_jaffle = pytest.mark.skipif(
    not JAFFLE.exists() and not os.environ.get("DBT2DATAFORM_REQUIRE_EXAMPLE"),
    reason="jaffle-shop checkout not found",
)


def convert(project_dir: Path, profiles_dir: Path | None = None, **options):
    project = load_project(project_dir, profiles_dir=profiles_dir)
    try:
        converter = Converter(project, Options(**options))
        return converter, converter.convert()
    finally:
        project.close()


def compile_dataform(files: dict[str, str], out: Path) -> dict:
    if os.environ.get("DBT2DATAFORM_SKIP_COMPILE") or not shutil.which("npx"):
        pytest.skip("Dataform compile disabled or npx missing")
    write_output(files, out, force=False)
    proc = subprocess.run(
        ["npx", "-y", DATAFORM_CLI, "compile", "--json"],
        cwd=out,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    compiled = json.loads(proc.stdout)
    assert not compiled.get("graphErrors"), compiled["graphErrors"]
    return compiled


def issues(converter, level: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for i in converter.report.issues:
        if i.level == level:
            out.setdefault(converter.report.short(i.subject), []).append(i.message)
    return out


@pytest.fixture(scope="module")
def edge():
    return convert(EDGE)


def test_models_render_as_dbt_bigquery_compiles_them(edge):
    _, files = edge
    fct = files["definitions/marts/fct_orders.sqlx"]
    assert 'onSchemaChange: "EXTEND"' in fct and 'tags: ["nightly"]' in fct
    assert 'partitionBy: "DATE_TRUNC(order_date, MONTH)"' in fct and 'clusterBy: ["id"]' in fct
    assert "CAST(amount * 100 AS INT64) AS amount_cents" in fct  # project macro with return()
    assert "datetime_add(" in fct  # dbt.dateadd, as dbt-bigquery implements it
    assert "current_timestamp() AS loaded_at" in fct
    assert "${when(incremental(), `WHERE order_date > (SELECT MAX(order_date) FROM ${self()})" in fct
    assert "ADD PRIMARY KEY (id) NOT ENFORCED" in fct

    stg = files["definitions/staging/stg_orders.sqlx"]
    assert 'type: "view"' in stg
    assert 'schema: dataform.projectConfig.defaultSchema + "_staging"' in stg
    assert 'FROM ${ref("raw_shop", "orders")}' in stg
    assert "${dataform.projectConfig.vars.min_amount}" in stg


def test_macros_resolve_by_dbts_rules(edge):
    _, files = edge
    tour = files["definitions/marts/macro_tour.sqlx"]
    assert "UPPER('quiet') AS loud" in tour  # a package macro
    assert "CAST(2 * 100 AS INT64) AS own_namespace" in tour  # edge.cents()
    assert "'hello from edge' AS greeting" in tour  # dispatch search_order override
    assert "1 AS private_value" in tour  # a macro whose name starts with _
    assert "NULL AS maybe" in tour  # var('x', none)
    assert "TIMESTAMP('${new Date().toISOString()}')" in tour  # run_started_at
    assert 'FROM ${ref("calendar")}' in tour  # through the project's ref() override

    spine = files["definitions/marts/calendar.sqlx"]
    assert "from unnest(generate_array(1, datetime_diff(" in spine


def test_packages_are_converted_with_their_vars(edge):
    converter, files = edge
    pkg = files["definitions/packages/edge_pkg/pkg_model.sqlx"]
    assert 'schema: dataform.projectConfig.defaultSchema + "_pkg"' in pkg
    assert 'FROM ${ref("pkg_model")}' in files["definitions/marts/uses_pkg_model.sqlx"]
    # dbt gives the root project pkg_rate 1 and edge_pkg 5 (the root's scoped
    # value beats the package's own 3). Dataform has one set of vars, so the
    # package gets a var of its own.
    assert "${dataform.projectConfig.vars.pkg_rate} AS rate" in files["definitions/marts/root_rate.sqlx"]
    assert "${dataform.projectConfig.vars.edge_pkg__pkg_rate} AS rate" in pkg
    settings = files["workflow_settings.yaml"]
    assert 'pkg_rate: "1"' in settings and 'edge_pkg__pkg_rate: "5"' in settings
    assert "edge_pkg__pkg_rate" in issues(converter, "info")["var pkg_rate"][0]


def test_tests_keep_dbt_semantics(edge):
    converter, files = edge
    stg = files["definitions/staging/stg_orders.sqlx"]
    assert 'uniqueKeys: [["id"]]' in stg and 'nonNull: ["id"]' in stg
    # unique without not_null: dbt ignores NULLs, Dataform's uniqueKeys would not.
    unique = files["definitions/assertions/generic/unique_fct_orders_id.sqlx"]
    assert "where id is not null" in unique
    rel = files["definitions/assertions/generic/relationships_fct_orders_id__id__ref_stg_orders_.sqlx"]
    assert '(select * from ${ref("fct_orders")} where amount_cents > 0) dbt_subquery' in rel
    positive = files["definitions/assertions/generic/positive_fct_orders_amount_cents.sqlx"]
    assert "SELECT count(*) AS failures" in positive and "WHERE failures >5" in positive
    assert "definitions/assertions/generic/source_not_null_shop_orders_id.sqlx" in files
    warnings = issues(converter, "warning")
    assert any("severity: warn" in m for msgs in warnings.values() for m in msgs)


def test_sources_seeds_and_snapshots(edge):
    converter, files = edge
    events = files["definitions/sources/shop/events.sqlx"]
    assert "CREATE OR REPLACE EXTERNAL TABLE ${self()}" in events
    assert "uris = ['${dataform.projectConfig.vars.events_bucket}/events/*.parquet']" in events
    fresh = files["definitions/assertions/freshness/source_freshness_shop_orders.sqlx"]
    assert 'dbt2dataform.last_modified(ref("raw_shop", "orders"))' in fresh

    seed = files["definitions/seeds/people.sqlx"]
    # As dbt-bigquery loads it: an all-empty column is INT64.
    assert "STRUCT<id INT64, name STRING, joined DATE, active BOOL, score FLOAT64, note INT64>" in seed
    assert "ALTER TABLE ${self()} SET OPTIONS (description = 'people')" in seed

    snap = files["definitions/snapshots/orders_snapshot.sqlx"]
    assert 'type: "declaration"' in snap and 'schema: "snapshots"' in snap
    manual = issues(converter, "manual")
    assert "orders_snapshot" in manual and "unit tests" in manual


def test_what_cannot_be_converted_is_flagged(edge):
    converter, files = edge
    assert "disabled: true" in files["definitions/marts/needs_warehouse.sqlx"]
    assert "run_query()" in issues(converter, "manual")["needs_warehouse"][0]
    assert any("Needs manual work" in m for m in issues(converter, "warning")["after_warehouse"])
    contract = files["definitions/assertions/contracts/fct_orders_contract.sqlx"]
    for expected in ("'INT64'", "'DATE'", "'DATETIME'", "'TIMESTAMP'"):
        assert expected in contract


def test_every_test_that_will_not_run_is_named(edge):
    converter, _ = edge
    report = converter.report.render()
    section = report.split("## Tests that will not run")[1].split("\n## ")[0]
    assert "| `fct_orders.amounts_become_cents` | `fct_orders` | dbt unit test" in section
    tests_line = next(m for msgs in issues(converter, "info").values() for m in msgs if "dbt tests became" in m)
    assert tests_line.startswith("8 enabled dbt tests became")


def test_tests_of_disabled_models_are_named(tmp_path):
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: proj\nprofile: edge\nversion: '1.0'\nconfig-version: 2\n")
    (project / "models" / "live.sql").write_text("{% set r = run_query('select 1') %}\nSELECT 1 AS id")
    (project / "models" / "parked.sql").write_text("{{ config(enabled=false) }}\nSELECT 1 AS id")
    (project / "models" / "schema.yml").write_text(
        "models:\n"
        "  - name: live\n    columns:\n      - name: id\n        data_tests: [not_null, unique]\n"
        "  - name: parked\n    columns:\n      - name: id\n        data_tests: [not_null]\n"
    )
    converter, _ = convert(project, profiles_dir=EDGE)
    idle = {converter.report.short(test).split(".")[0]: (tested, why) for test, tested, why in converter.report.idle_tests}
    for name in ("not_null_live_id", "unique_live_id"):
        assert idle[name][0] == "model.proj.live" and "written disabled" in idle[name][1]
    infos = [m for msgs in issues(converter, "info").values() for m in msgs]
    assert any("2 of the converted ones test models written disabled" in m for m in infos)
    assert any("disabled in dbt, so not converted" in m and "test" in m for m in infos)


def test_edge_project_compiles(edge, tmp_path):
    _, files = edge
    compiled = compile_dataform(files, tmp_path / "edge")
    names = {t["target"]["name"] for t in compiled["tables"]}
    assert {"fct_orders", "stg_orders", "people", "calendar", "pkg_model"} <= names


def test_packages_can_be_left_out(tmp_path):
    converter, files = convert(EDGE, include_packages=False)
    assert "UPPER('quiet')" in files["definitions/marts/macro_tour.sqlx"]  # its macros still work
    # Only what the project itself uses is declared, so its own models still convert.
    assert [p for p in files if p.startswith("definitions/packages/")] == ["definitions/packages/edge_pkg/pkg_model.sqlx"]
    declared = files["definitions/packages/edge_pkg/pkg_model.sqlx"]
    assert 'type: "declaration"' in declared and 'name: "pkg_model"' in declared
    assert 'schema: dataform.projectConfig.defaultSchema + "_pkg"' in declared
    uses = files["definitions/marts/uses_pkg_model.sqlx"]
    assert "disabled" not in uses and 'FROM ${ref("pkg_model")}' in uses
    compiled = compile_dataform(files, tmp_path / "no_packages")
    assert "uses_pkg_model" in {t["target"]["name"] for t in compiled["tables"]}


def test_refuses_other_adapters(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("name: other\nprofile: other\n")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": {}}))
    with pytest.raises(ProjectError, match="dbt-bigquery projects only"):
        load_project(tmp_path, manifest_path=manifest)


def test_rerun_replaces_only_its_own_files(edge, tmp_path):
    _, files = edge
    out = tmp_path / "out"
    write_output(files, out, force=False)
    (out / "definitions" / "hand_written.sqlx").write_text("SELECT 1")
    write_output({k: v for k, v in files.items() if "regions" not in k}, out, force=False)
    assert not (out / "definitions" / "marts" / "regions.sqlx").exists()
    assert (out / "definitions" / "hand_written.sqlx").exists()


def test_keeps_a_gitignore_it_did_not_write(edge, tmp_path):
    _, files = edge
    (tmp_path / ".gitignore").write_text("data/\n")
    (tmp_path / "empty_but_not_ours").mkdir()
    write_output(files, tmp_path, force=True)
    write_output(files, tmp_path, force=False)
    assert (tmp_path / ".gitignore").read_text() == "data/\n"
    assert (tmp_path / "empty_but_not_ours").is_dir()
    assert ".gitignore" not in json.loads((tmp_path / ".dbt2dataform.json").read_text())["files"]


def test_refuses_a_foreign_non_empty_directory(edge, tmp_path):
    (tmp_path / "keep.txt").write_text("x")
    with pytest.raises(FileExistsError):
        write_output(edge[1], tmp_path, force=False)


OVERRIDES = {
    "*": {"tags": ["daily"]},
    "marts/*": {"tags": ["marts"]},
    "fct_orders": {"bigquery": {"partitionBy": "order_date"}, "description": None},
    "stg_orders": {"type": "table"},
    "no_such_model": {"tags": ["x"]},
}


def test_overrides(tmp_path):
    converter, files = convert(EDGE, overrides=OVERRIDES)
    fct = files["definitions/marts/fct_orders.sqlx"]
    assert 'tags: ["nightly", "daily", "marts"]' in fct  # dbt's tag first, then the overrides
    assert 'partitionBy: "order_date"' in fct and 'clusterBy: ["id"]' in fct  # mappings merge
    assert "description:" not in fct
    stg = files["definitions/staging/stg_orders.sqlx"]
    assert 'type: "table"' in stg and 'tags: ["daily"]' in stg
    assert "tags" not in files["definitions/sources/shop/orders.sqlx"]  # declarations take no tags
    assert 'tags: ["daily"]' in files["definitions/assertions/one_row_per_order.sqlx"]
    assert "override `no_such_model` matched no action." in issues(converter, "warning")["dbt2dataform.yml"]
    compiled = compile_dataform(files, tmp_path / "overrides")
    fct_compiled = next(t for t in compiled["tables"] if t["target"]["name"] == "fct_orders")
    assert fct_compiled["bigquery"]["partitionBy"] == "order_date"


def test_settings_file(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "dbt2dataform.yml").write_text(
        "default_project: from-file\n"
        "default_location: EU\n"
        "vars: {min_amount: 25, dbt_env_label: prod}\n"
        "overrides:\n"
        "  fct_orders: {bigquery: {clusterBy: [order_date]}}\n"
    )
    assert main([str(EDGE), str(out), "--force", "--var", "dbt_env_label=cli"]) == 0
    settings = (out / "workflow_settings.yaml").read_text()
    assert "defaultProject: from-file" in settings and "defaultLocation: EU" in settings
    assert 'min_amount: "25"' in settings and 'dbt_env_label: "cli"' in settings  # flags win
    assert 'clusterBy: ["order_date"]' in (out / "definitions/marts/fct_orders.sqlx").read_text()
    (out / "dbt2dataform.yml").write_text("default_projct: typo\n")
    assert main([str(EDGE), str(out)]) == 2


@needs_jaffle
def test_jaffle_shop(tmp_path):
    _, files = convert(JAFFLE, profiles_dir=PROFILES)
    disabled = [p for p, c in files.items() if "disabled: true" in c]
    assert not disabled, disabled
    stg = files["definitions/staging/stg_orders.sqlx"]
    assert "round(cast((order_total / 100) as numeric), 2)" in stg  # bigquery__cents_to_dollars
    compiled = compile_dataform(files, tmp_path / "jaffle")
    tables = {t["target"]["name"] for t in compiled["tables"]}
    assert {"orders", "customers", "metricflow_time_spine"} <= tables


@needs_jaffle
def test_committed_example_is_current():
    """examples/jaffle_shop is jaffle-shop's conversion; regenerate it when this fails:

    git clone https://github.com/dbt-labs/jaffle-shop ../jaffle-shop
    git -C ../jaffle-shop checkout 5beb145b00f5465ec759cfcdd9745e858818cf95
    uv run dbt2dataform ../jaffle-shop examples/jaffle_shop --profiles-dir tests/fixtures/profiles
    """
    _, files = convert(JAFFLE, profiles_dir=PROFILES)
    stale = [rel for rel, content in files.items() if (EXAMPLE / rel).read_text() != content]
    assert not stale, f"regenerate the example; changed: {stale}"
