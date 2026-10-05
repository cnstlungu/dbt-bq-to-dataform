# dbt2dataform

[![CI](https://github.com/cnstlungu/dbt2dataform/actions/workflows/ci.yml/badge.svg)](https://github.com/cnstlungu/dbt2dataform/actions/workflows/ci.yml)

Converts a **dbt-bigquery** project into a Dataform project. It converts
models, seeds, sources, tests, contracts, source freshness, hooks, vars and
incremental logic. It also writes a `CONVERSION_REPORT.md` that lists
everything it could not carry over, or carried over with different
behaviour.

The converter does not translate SQL. dbt's own macros render every model,
using dbt-bigquery's implementations and dbt's rules for resolving and
dispatching macros, so the SQL is what dbt would compile. Only the dbt
constructs around it become Dataform: `ref()`, `source()`, `this`, vars,
`is_incremental()`, configs and tests.

## Scope

- **dbt-bigquery projects only.** Dataform runs on BigQuery. A project on
  another adapter is refused with an explanation. Translating SQL between
  warehouses is a separate job: move the project to dbt-bigquery first, then
  convert it. BigQuery's SQL translation service can help with the first step.
- **dbt-core 1.8 to 1.12**, which all write the v12 manifest. Development and
  CI use dbt-core 1.12.5 with dbt-bigquery 1.12.1. The test fixture was also
  converted with each minor release from 1.8.10 to 1.11.15, each paired with
  its matching dbt-bigquery. The Dataform output was identical every time.
  Other versions get a warning in the report. dbt 2.x (the Rust engine) is
  not supported.
- **Dataform core 3.0.71 or newer.** Older CLIs silently drop config such as
  `onSchemaChange`, even when `dataformCoreVersion` asks for a newer core.

## Quick start

```bash
uvx --from git+https://github.com/cnstlungu/dbt2dataform@v0.1.0 \
    --with dbt-core --with dbt-bigquery \
    dbt2dataform path/to/dbt_project out/dataform_project
```

The tool runs `dbt deps` and `dbt parse` on a temporary copy of the project,
so your checkout is never written to. Packages installed from a relative
path (`local: ../pkg`) still resolve. Parsing doesn't connect to BigQuery,
so a profile with `method: oauth` and any project ID will do. Pass
`--profiles-dir`, `--profile` and `--target` to choose one. The target
matters because schema and database names are resolved for it.

Env vars the project needs at parse time get placeholders. To reuse a
manifest you already have, pass `--manifest target/manifest.json`, and make
sure `dbt_packages/` is present so package seeds can be read.

Then compile the result:

```bash
cd out/dataform_project && npx @dataform/cli@3.0.71 compile
```

Running the tool again into the same directory replaces only the files the
previous run wrote. It records them in `.dbt2dataform.json`, and keeps a
`.gitignore` that was already there.

## What becomes what

| dbt | Dataform |
|---|---|
| `table` / `view` / `incremental` model | `table` / `view` / `incremental` action |
| `materialized_view` | `view` with `materialized: true` |
| `ephemeral` model | `view` (warning) |
| models, seeds, sources and tests of installed packages | the same, under `definitions/packages/<package>/` (`--no-packages` leaves them out) |
| `{{ ref() }}`, `{{ source() }}`, `{{ this }}` | `${ref()}`, `${self()}` |
| `{% if is_incremental() %}…{% else %}…{% endif %}` | `${when(incremental(), …, …)}` around only the lines that differ |
| any macro (project, package or dbt's), `adapter.dispatch` | rendered at conversion time, as dbt-bigquery would compile it |
| `unique_key`, `on_schema_change`, `incremental_predicates` | `uniqueKey`, `onSchemaChange`, `incrementalPredicates` |
| `incremental_strategy: merge` / `insert_overwrite` | MERGE / `incrementalStrategy: "INSERT_OVERWRITE"` |
| `partition_by`, `cluster_by`, `labels`, `partition_expiration_days`, `require_partition_filter` | `bigquery: {…}` |
| `full_refresh: false` | `protected: true` |
| custom `schema` with dbt's default `generate_schema_name` | `schema: dataform.projectConfig.defaultSchema + "_<custom>"` |
| any other resolved schema (e.g. a custom `generate_schema_name`) | the dataset name, written literally |
| scalar `var('x')` | `${dataform.projectConfig.vars.x}`, its value in `workflow_settings.yaml` |
| `env_var('X')` | Dataform var `x` (warning: Dataform does not read the environment) |
| `pre_hook` / `post_hook` / `sql_header` | `pre_operations` / `post_operations` |
| `not_null`, `accepted_values`, `dbt_utils.expression_is_true`, `dbt_utils.accepted_range`, `dbt_utils.unique_combination_of_columns`; `unique` on a column that is also `not_null` | built-in `assertions: {nonNull, rowConditions, uniqueKeys}` |
| every other generic test, including custom and package ones | an assertion file holding the SQL of the test's own macro |
| tests with `where`, `severity`, `error_if` / `warn_if` / `fail_calc` / `limit` | an assertion file; non-default thresholds become a count that returns a row exactly when dbt would fail |
| singular tests | assertion files |
| model contracts | an assertion that checks `INFORMATION_SCHEMA.COLUMNS`, plus `PRIMARY KEY … NOT ENFORCED` |
| source freshness (`error_after`) | an assertion tagged `source_freshness`, on `loaded_at_field` or, without one, the table's last-modified time as dbt-bigquery reads it |
| sources | `declaration`s; dbt-external-tables sources (`external:`) become `CREATE OR REPLACE EXTERNAL TABLE` operations |
| seeds | a table built from an inline typed array, typed as dbt-bigquery would load it; large seeds become `LOAD DATA` from GCS |
| snapshots | a `declaration` of the table dbt's snapshot maintains (manual item) |
| descriptions, column descriptions, `policy_tags`, tags | `description`, `columns` (with `bigqueryPolicyTags`), `tags` |

## Limitations

Everything below is listed in the conversion report when a project uses it.
The only settings dropped without a note are those that change nothing in
BigQuery: `meta`, `docs`, `quoting`, `persist_docs`, and the tags of sources
(Dataform declarations take none).

### Needs a live warehouse

A conversion is static: it never connects to BigQuery. When a model's Jinja
asks the warehouse something, the model is written `disabled: true`, with its
dbt SQL kept in comments, and models downstream of it are flagged. This
affects:

- `run_query`, `statement` blocks and `load_result`
- adapter introspection: `adapter.get_columns_in_relation`,
  `adapter.get_relation` and the like

That covers several common macros:

- `dbt_utils`: `star`, `union_relations`, `get_column_values`,
  `get_single_value`, `get_relations_by_pattern`, and `pivot` when it gets
  its values from `get_column_values`
- Fivetran packages: `fill_staging_columns` and `union_connections`

`dbt.date_spine` and `dbt_utils.date_spine` are the exception. dbt runs a
query only to count the intervals between two dates. Here `GENERATE_ARRAY`
computes the same count inside the SQL, so the rows are the same.

### No Dataform equivalent

These are reported, not converted:

- **Snapshots.** Each is declared, so models that ref it still resolve, but
  the SCD2 logic is not ported.
- **Python models and dbt unit tests.** Dataform's `type: "test"` actions
  could hold the unit tests, but they are not converted yet.
- **User-defined functions** (dbt 1.11+ `functions`).
- **The `microbatch` strategy.**
- **Hooks and permissions:** `on-run-start` / `on-run-end` hooks, `grants`
  and `grant_access_to`.
- **dbt-bigquery configs:** `kms_key_name`, `hours_to_expiration`,
  `merge_update_columns`, `merge_exclude_columns`, `copy_partitions`,
  ingestion-time partitioning, and `on_configuration_change`.
- **Model governance:** groups, access and deprecation dates.
- **Semantic layer and documentation:** exposures, metrics, semantic models,
  saved queries and analyses.

### Converted with different behaviour

- **`insert_overwrite` with static `partitions`** becomes Dataform's dynamic
  INSERT_OVERWRITE. It replaces every partition the query returns.
- **Tests with `severity: warn`** become assertions, and assertions fail.
  Freshness `warn_after` is dropped, and `loaded_at_query` freshness is not
  converted.
- **`store_failures`** is dropped, because Dataform keeps every assertion's
  failing rows as a view anyway. Several built-in assertions on one table
  share one Dataform action, so their individual test names are lost.
- **Ephemeral models** become views.
- **Descriptions** are always written to BigQuery. dbt writes them only with
  `persist_docs`.

### Evaluated once, at conversion time

- **`target.*`.** `target.type` is always `bigquery`, so it's exact.
  Anything else read from `target` is fixed to the parse target, with a
  warning.
- **Vars.** Scalar vars become Dataform vars. Lists, mappings and booleans
  are inlined, because Dataform vars are strings. A var whose value is Jinja
  is rendered once. `env_var()` becomes a Dataform var with the default as
  its value.
- **`run_started_at`.** Printed as is, it becomes the time Dataform compiles
  the project. Anything computed from it, such as `.strftime()` or
  arithmetic, is fixed at conversion. `invocation_id` disables the model.
- **Schema and database names** come from the parse target. They include
  whatever a custom `generate_schema_name` returned for that target.

### Not verified against data

The tests check that conversions render through dbt's macros and compile in
Dataform. They don't run the result on BigQuery. A converted project is only
as correct as its SQL, which is the SQL dbt would run. Run it against a copy
of your data before you switch.

## Results on public projects

Conversions of public dbt-bigquery projects on 2026-10-05, with packages
included:

| Project | Models converted | Not converted (why) |
|---|---|---|
| [dbt-labs/jaffle-shop](https://github.com/dbt-labs/jaffle-shop) | 13 of 13 | none. Its 3 unit tests and its semantic layer are reported. |
| [tuva-health/tuva](https://github.com/tuva-health/tuva) `integration_tests` | 528 of 571 | 43: column and relation introspection (35), `union_relations` (7), `invocation_id` (1) |
| [fivetran/dbt_stripe](https://github.com/fivetran/dbt_stripe) `integration_tests` | 14 of 65 | 51: warehouse introspection, mostly Fivetran's `fill_staging_columns` and `union_connections` (48), plus `dbt_utils` macros that query (3) |

[`examples/jaffle_shop`](examples/jaffle_shop) is jaffle-shop's conversion.
CI checks that it is current.

## Settings file and overrides

A `dbt2dataform.yml` in the output directory, or one passed with `--config`,
holds the flags, so a generated project can be regenerated with one command.
Command-line flags win over it. It can also add Dataform config that has no
dbt counterpart, such as schedule tags:

```yaml
default_project: my-gcp-project
default_location: EU
vars:
  input_files_path: gs://my-bucket/raw

overrides:
  "*":                      # every action
    tags: [schedule_daily]
  "staging/*":              # a `/` matches the file path under definitions/
    tags: [staging]
  fct_orders:               # anything else matches the action name; globs work
    bigquery:
      clusterBy: [customer_id]
```

Every matching override applies, in file order:
- Mappings merge.
- `tags` are added to.
- `null` removes a key.
- Anything else replaces.

Declarations never get tags, because Dataform does not allow them. An
override that matches nothing is reported as a warning.

The other keys are `core_version`, `contracts` and `packages`. They mirror
`--core-version`, `--no-contracts` and `--no-packages`.

## How it works

1. **Manifest.** `dbt parse` resolves configs, refs, tests and packages.
   Values dbt bakes into the manifest at parse time, such as `env_var()` in
   source configs, are read back from the YAML.
2. **Jinja.** Every macro in the manifest is loaded, resolved by dbt's rules:
   - The model's own package comes first, then the root project, package
     namespaces, `dbt.*`, and dbt's built-ins.
   - `adapter.dispatch` follows the project's `dispatch` search order, then
     looks for `bigquery__`, then `default__`.

   `ref`, `source`, `this` and vars render as placeholder tokens, which
   macros pass around like relation names. Anything that needs a warehouse
   raises, and the model is flagged.
3. **Tests.** Generic tests render through their own test macros, with
   arguments rendered the way dbt renders them. dbt's failure thresholds are
   then applied around the result.
4. **SQLX.** An incremental model is rendered twice and the two renders are
   diffed line by line. Only the lines that differ are wrapped in
   `when(incremental(), …)`, so each branch compiles to exactly what dbt
   would run. Tokens become JavaScript.

## Development

```bash
uv run pytest
```

The tests do two things:
- **Fixture.** They convert the edge fixture
  ([`tests/fixtures/edge_project`](tests/fixtures/edge_project), with a local
  package) and compile the result with the Dataform CLI through `npx`.
- **Golden example.** They do the same for jaffle-shop. It is read from
  `../jaffle-shop`, or from `DBT2DATAFORM_EXAMPLE`, and skipped when missing.
  CI checks it out at a pinned commit and sets
  `DBT2DATAFORM_REQUIRE_EXAMPLE=1`.

Set `DBT2DATAFORM_SKIP_COMPILE=1` to skip compiling.
`test_committed_example_is_current` fails when the converter's output
changes; its docstring has the command that regenerates the example.
