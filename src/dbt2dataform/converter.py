"""Turns a loaded dbt-bigquery project into the files of a Dataform project."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from . import seeds as seedlib
from .dbt_project import DbtProject, dbt_version_supported
from .errors import Unsupported
from .jinja_render import Renderer
from .report import Mapping, Report
from .settings import SETTINGS_FILE, Overrides
from .sqlx import JS, commented, js_string, merge_incremental, replace_tokens, sqlx_file, tidy_sql
from .tokens import TokenRegistry

RESOURCES = Path(__file__).parent / "resources"
STATE_FILE = ".dbt2dataform.json"
DEFAULT_LOCATION = "US"
# Older CLIs and cores silently drop config such as onSchemaChange.
DEFAULT_CORE_VERSION = "3.0.71"

_ON_SCHEMA_CHANGE = {
    "fail": "FAIL",
    "append_new_columns": "EXTEND",
    "sync_all_columns": "SYNCHRONIZE",
}
_MATERIALIZATIONS = {
    "table": "table",
    "view": "view",
    "incremental": "incremental",
    "ephemeral": "view",
    "materialized_view": "view",
}
_PERIOD = {"minute": "MINUTE", "hour": "HOUR", "day": "DAY"}
_FORMATS = {
    "parquet": "PARQUET",
    "csv": "CSV",
    "json": "NEWLINE_DELIMITED_JSON",
    "newline_delimited_json": "NEWLINE_DELIMITED_JSON",
    "avro": "AVRO",
    "orc": "ORC",
    "google_sheets": "GOOGLE_SHEETS",
    "datastore_backup": "DATASTORE_BACKUP",
}
# dbt-bigquery model configs with no Dataform counterpart.
_UNMAPPED_CONFIG = {
    "kms_key_name": "CMEK encryption (kms_key_name)",
    "hours_to_expiration": "table expiration (hours_to_expiration)",
    "grant_access_to": "authorized views (grant_access_to)",
    "merge_update_columns": "merge_update_columns",
    "merge_exclude_columns": "merge_exclude_columns",
    "copy_partitions": "copy_partitions",
}
# Tests whose dbt semantics Dataform's built-in assertions reproduce exactly.
_INLINE_TESTS = {
    (None, "not_null"),
    (None, "unique"),
    (None, "accepted_values"),
    ("dbt_utils", "unique_combination_of_columns"),
    ("dbt_utils", "expression_is_true"),
    ("dbt_utils", "accepted_range"),
}


@dataclass
class Options:
    default_project: str | None = None
    default_location: str = DEFAULT_LOCATION
    core_version: str = DEFAULT_CORE_VERSION
    contracts: bool = True
    var_overrides: dict[str, str] = field(default_factory=dict)
    # selector -> Dataform config; see settings.Overrides
    overrides: dict[str, dict] = field(default_factory=dict)
    # Convert the models, seeds, sources and tests of installed packages too,
    # as `dbt build` builds them.
    include_packages: bool = True
    target_name: str | None = None


@dataclass
class Target:
    """The Dataform action a dbt relation (model, seed, snapshot, source) becomes."""

    unique_id: str
    name: str
    schema: str  # resolved dataset name
    schema_config: str | JS | None  # what to write as `schema:`; None = default dataset
    database: str | None = None


class Converter:
    def __init__(self, project: DbtProject, options: Options):
        self.project = project
        self.options = options
        self.report = Report(project_name=project.name, dbt_version=project.dbt_version)
        self.registry = TokenRegistry()
        self.renderer = Renderer(project, self.registry, self.report, options.target_name)
        self.files: dict[str, str] = {}
        self.targets: dict[str, Target] = {}
        self.inline_assertions: dict[str, dict[str, list]] = defaultdict(
            lambda: {"uniqueKeys": [], "nonNull": [], "rowConditions": []}
        )
        self.includes_used: set[str] = set()
        self.overrides = Overrides(options.overrides)
        # unique_id -> why it was written disabled or not converted
        self.failed: dict[str, str] = {}
        self.converted: set[str] = set()
        self.default_schema = self._default_schema()
        self.default_database = self._default_database()

    # -- driver -----------------------------------------------------------------

    def convert(self) -> dict[str, str]:
        version = self.project.dbt_version
        if not dbt_version_supported(version):
            self.report.warning(
                "dbt",
                f"the manifest comes from dbt-core {version}; dbt2dataform is tested "
                "with dbt-core 1.8 to 1.12.",
            )
        for env_name in self.project.parse_env:
            self.report.info(
                "dbt parse",
                f"env var `{env_name}` was unset, so dbt parse ran with a placeholder for it.",
            )
        self._plan_targets()
        self._convert_sources()
        self._declare_left_out()
        self._collect_tests()
        self._convert_seeds()
        self._convert_models()
        self._convert_tests()
        self._flag_broken_dependencies()
        self._note_unconverted()
        for selector in self.overrides.unused():
            self.report.warning(SETTINGS_FILE, f"override `{selector}` matched no action.")
        self._write_project_files()
        return self.files

    # -- planning ---------------------------------------------------------------

    def _in_scope(self, resource_type: str) -> list[dict]:
        return self.project.of_type(resource_type, self.options.include_packages)

    def _default_schema(self) -> str:
        root = [n for n in self.project.nodes.values() if n["package_name"] == self.project.name]
        for n in sorted(root, key=lambda n: n["unique_id"]):
            if n["resource_type"] not in ("model", "seed"):
                continue
            custom = n["config"].get("schema")
            if not custom:
                return n["schema"]
            if n["schema"].endswith("_" + custom):
                return n["schema"][: -len(custom) - 1]
        return self.project.name

    def _default_database(self) -> str | None:
        dbs = Counter(
            n["database"]
            for n in self.project.nodes.values()
            if n["resource_type"] == "model" and n.get("database")
        )
        return dbs.most_common(1)[0][0] if dbs else None

    def _schema_config(self, node: dict) -> str | JS | None:
        custom = node["config"].get("schema")
        resolved = node["schema"]
        if not custom and resolved == self.default_schema:
            return None
        if custom and resolved == f"{self.default_schema}_{custom}":
            # dbt's default generate_schema_name: <target schema>_<custom>.
            return JS(f'dataform.projectConfig.defaultSchema + "_{custom}"')
        self.report.info(
            node["unique_id"],
            f"schema `{resolved}` does not follow dbt's default naming "
            "(a custom generate_schema_name?), so it was written as is.",
        )
        return resolved

    def _database_config(self, node: dict) -> str | None:
        db = node.get("database")
        return db if db and db != self.default_database else None

    def _plan_targets(self) -> None:
        for kind in ("model", "seed", "snapshot"):
            for n in self._in_scope(kind):
                self.targets[n["unique_id"]] = Target(
                    unique_id=n["unique_id"],
                    name=n.get("alias") or n["name"],
                    schema=n["schema"],
                    # A snapshot's target_schema is absolute, not a custom suffix.
                    schema_config=n["schema"] if kind == "snapshot" else self._schema_config(n),
                    database=self._database_config(n),
                )
        # With --no-packages, the packages' nodes the project itself uses stay
        # reachable as declarations of the tables dbt builds for them.
        self._left_out = self._left_out_dependencies()
        for uid in self._left_out:
            n = self.project.nodes.get(uid)
            if n is not None:
                self.targets[uid] = Target(
                    unique_id=uid,
                    name=n.get("alias") or n["name"],
                    schema=n["schema"],
                    schema_config=n["schema"] if n["resource_type"] == "snapshot" else self._schema_config(n),
                    database=self._database_config(n),
                )
        for s in self.project.sources.values():
            self.targets[s["unique_id"]] = Target(
                unique_id=s["unique_id"],
                name=s.get("identifier") or s["name"],
                schema=s["schema"],
                schema_config=s["schema"],
                database=self._database_config(s),
            )
        self._name_counts = Counter(t.name for t in self.targets.values())

    def _left_out_dependencies(self) -> list[str]:
        """Package nodes and sources outside the conversion that converted nodes use."""
        if self.options.include_packages:
            return []
        inside = lambda n: self.project.in_scope(n, False)  # noqa: E731
        used = {
            dep
            for n in self.project.nodes.values()
            if inside(n) and n["resource_type"] in ("model", "seed", "snapshot", "test")
            for dep in n.get("depends_on", {}).get("nodes", [])
        }
        left_out = []
        for dep in sorted(used):
            node = self.project.nodes.get(dep) or self.project.sources.get(dep)
            if node is not None and not inside(node) and node["resource_type"] in ("model", "seed", "snapshot", "source"):
                left_out.append(dep)
        return left_out

    def _declare_left_out(self) -> None:
        for uid in self._left_out:
            node = self.project.nodes.get(uid) or self.project.sources[uid]
            target = self.targets[uid]
            kind = node["resource_type"]
            if kind == "source":
                path = f"{self._base(node)}/sources/{node['source_name']}/{target.name}.sqlx"
            else:
                folder = {"model": "", "seed": "seeds/", "snapshot": "snapshots/"}[kind]
                rel = self._relative(node, kind)
                path = f"{self._base(node)}/{folder}{rel.with_suffix('.sqlx')}"
            cfg = {
                "type": "declaration",
                "schema": target.schema_config,
                "name": target.name,
                "database": target.database,
                "description": node.get("description") or None,
            }
            self._emit(path, cfg, "", node, "declared: left out with --no-packages")
            self.report.info(
                uid,
                "left out with --no-packages and declared, so the project's own models "
                "that use it read the table dbt builds for it.",
            )

    # -- JS for tokens ----------------------------------------------------------

    def _ref_js(self, unique_id: str) -> str:
        target = self.targets.get(unique_id)
        if target is None:
            raise Unsupported(f"{unique_id} is not part of the conversion")
        is_source = unique_id.startswith("source.")
        if not is_source and self._name_counts[target.name] == 1:
            return f"ref({js_string(target.name)})"
        schema = target.schema_config if target.schema_config is not None else JS("dataform.projectConfig.defaultSchema")
        schema_js = str(schema) if isinstance(schema, JS) else js_string(schema)
        return f"ref({schema_js}, {js_string(target.name)})"

    def _to_js(self, rep) -> str:
        if rep.kind == "ref":
            return self._ref_js(rep.payload[0])
        if rep.kind == "self":
            return "self()"
        if rep.kind == "var":
            return f"dataform.projectConfig.vars.{rep.payload[0]}"
        if rep.kind == "js":
            return rep.payload[0]
        raise Unsupported(f"no JavaScript for {rep}")

    def _finish(self, text: str) -> str:
        return replace_tokens(text, self.registry.get, self._to_js)

    # -- rendering --------------------------------------------------------------

    def _sql(self, raw: str, node: dict) -> str:
        """Render one piece of dbt SQL, both incremental branches if it has them."""
        full = self.renderer.render(raw, node, incremental=False)
        text = tidy_sql(full.text)
        if full.used_is_incremental and node.get("config", {}).get("materialized") == "incremental":
            inc = self.renderer.render(raw, node, incremental=True)
            text = merge_incremental(text, tidy_sql(inc.text))
        return self._finish(text)

    def _hooks(self, node: dict) -> tuple[list[str], list[str]]:
        cfg = node["config"]
        pre = [s for s in (self._sql(h["sql"], node) for h in cfg.get("pre-hook") or []) if s.strip()]
        post = [s for s in (self._sql(h["sql"], node) for h in cfg.get("post-hook") or []) if s.strip()]
        if cfg.get("sql_header"):
            pre.insert(0, self._sql(cfg["sql_header"], node))
            self.report.warning(node["unique_id"], "sql_header moved into pre_operations.")
        return pre, post

    # -- sources ----------------------------------------------------------------

    def _convert_sources(self) -> None:
        for s in self.project.sources_in_scope(self.options.include_packages):
            uid = s["unique_id"]
            target = self.targets[uid]
            _src_block, table_block = self.project.raw_source_entry(s)
            base_cfg = {
                "schema": target.schema,
                "name": target.name,
                "database": target.database,
                "description": s.get("description") or s.get("source_description") or None,
                "columns": self._columns(s),
            }
            path = f"{self._base(s)}/sources/{s['source_name']}/{target.name}.sqlx"
            external = table_block.get("external") or s.get("external") or {}
            if external.get("location") or (external.get("options") or {}).get("uris"):
                try:
                    body = self._external_table_sql(s, external)
                except Unsupported as exc:
                    self.report.manual(uid, f"external table not converted: {exc}")
                    body = None
                if body is not None:
                    cfg = {"type": "operations", **base_cfg, "tags": list(s.get("tags") or []), "hasOutput": True}
                    self._emit(path, cfg, body, s, "external table (dbt-external-tables)")
                    self._convert_freshness(s)
                    continue
            self._emit(path, {"type": "declaration", **base_cfg}, "", s)
            self._convert_freshness(s)

    def _external_table_sql(self, source: dict, external: dict) -> str:
        options = dict(external.get("options") or {})
        rendered = {k: self._sql(str(v), source).strip() for k, v in options.items() if isinstance(v, str)}
        options.update(rendered)
        uris = options.pop("uris", None)
        if uris is None:
            uris = [self._sql(str(external["location"]), source).strip()]
        elif isinstance(uris, str):
            uris = [uris]
        fmt = str(options.pop("format", "") or external.get("file_format") or "")
        if not fmt:
            suffix = PurePosixPath(uris[0].split("?")[0].rstrip("*")).suffix.lstrip(".").lower()
            fmt = suffix
        fmt = _FORMATS.get(fmt.lower(), fmt.upper()) if fmt else ""
        if not fmt:
            raise Unsupported(f"could not tell the file format of {uris[0]}")
        columns = [
            (c.get("name"), c.get("data_type"))
            for c in (external.get("columns") or source.get("columns", {}).values() or [])
            if isinstance(c, dict) and c.get("data_type")
        ]
        column_sql = ""
        if columns:
            column_sql = " (\n" + ",\n".join(f"  {n} {t}" for n, t in columns) + "\n)"
        partition_sql = ""
        partitions = external.get("partitions") or []
        if options.get("hive_partition_uri_prefix") or partitions:
            cols = [f"{p['name']} {p['data_type']}" for p in partitions if p.get("name") and p.get("data_type")]
            partition_sql = "\nWITH PARTITION COLUMNS" + (f" ({', '.join(cols)})" if cols else "")
        option_sql = [f"  format = '{fmt}'", f"  uris = [{', '.join(_sql_string(u) for u in uris)}]"]
        for key, value in options.items():
            option_sql.append(f"  {key} = {_option_value(value)}")
        return (
            "CREATE SCHEMA IF NOT EXISTS `${database()}.${schema()}`;\n\n"
            f"CREATE OR REPLACE EXTERNAL TABLE ${{self()}}{column_sql}{partition_sql}\n"
            "OPTIONS (\n" + ",\n".join(option_sql) + "\n)\n"
        )

    def _convert_freshness(self, s: dict) -> None:
        uid = s["unique_id"]
        cfg = s.get("config") or {}
        freshness = cfg.get("freshness") or s.get("freshness") or {}
        loaded_at = cfg.get("loaded_at_field") or s.get("loaded_at_field")
        error_after = (freshness or {}).get("error_after") or {}
        warn_after = (freshness or {}).get("warn_after") or {}
        if not (error_after.get("count") or warn_after.get("count")):
            return
        if cfg.get("loaded_at_query") or s.get("loaded_at_query"):
            self.report.manual(uid, "loaded_at_query freshness was not converted.")
            return
        if not error_after.get("count"):
            self.report.warning(uid, "freshness has warn_after only; Dataform assertions cannot warn, so none was written.")
            return
        if warn_after.get("count"):
            self.report.info(uid, "freshness warn_after dropped: Dataform assertions fail or pass, they do not warn.")
        period = _PERIOD.get(error_after.get("period", "day"), "DAY")
        target = self.targets[uid]
        ref = self._ref_js(uid)
        threshold = f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(error_after['count'])} {period})"
        if loaded_at:
            where = f"\nWHERE {freshness['filter']}" if freshness.get("filter") else ""
            body = (
                f"SELECT MAX(CAST({loaded_at} AS TIMESTAMP)) AS max_loaded_at\n"
                f"FROM ${{{ref}}}{where}\n"
                f"HAVING MAX(CAST({loaded_at} AS TIMESTAMP)) < {threshold}\n"
            )
        else:
            # No loaded_at_field: dbt-bigquery reads the table's last-modified time.
            self.includes_used.add("dbt2dataform")
            body = (
                f"SELECT last_modified\n"
                f"FROM (SELECT ${{dbt2dataform.last_modified({ref})}} AS last_modified)\n"
                f"WHERE last_modified < {threshold}\n"
            )
        name = f"source_freshness_{s['source_name']}_{target.name}"
        period_text = f"{error_after['count']} {error_after.get('period', 'day')}(s)"
        cfg_out = {
            "type": "assertion",
            "name": name,
            "tags": ["source_freshness"],
            "description": f"Port of `dbt source freshness` for {s['source_name']}.{s['name']}: fails when its data is older than {period_text}.",
        }
        body = f"-- dbt source freshness, error_after {period_text}.\n" + body
        self._emit(f"{self._base(s)}/assertions/freshness/{name}.sqlx", cfg_out, body, s, "source freshness")

    # -- seeds ------------------------------------------------------------------

    def _convert_seeds(self) -> None:
        for n in self._in_scope("seed"):
            uid = n["unique_id"]
            target = self.targets[uid]
            csv_path = self.project.file_of(n)
            rel = self._relative(n, "seed")
            path = f"{self._base(n)}/seeds/{rel.with_suffix('.sqlx')}"
            if csv_path is None or not csv_path.exists():
                self._fail(uid, "the seed's CSV file was not found next to the manifest")
                self.report.manual(uid, "seed not converted: its CSV file was not found.")
                continue
            cfg_in = n["config"]
            seed = seedlib.read_seed(csv_path, cfg_in.get("column_types") or {}, cfg_in.get("delimiter") or ",")
            if not seed.inferred_by_dbt:
                self.report.info(
                    uid,
                    "seed column types follow dbt's inference rules as written in "
                    "dbt2dataform; install dbt-bigquery alongside it to use dbt's own.",
                )
            try:
                pre, post = self._hooks(n)
            except Unsupported as exc:
                empty = " The CSV has no rows, so the table stays empty until it is." if not seed.rows else ""
                self.report.manual(uid, f"seed hooks not converted: {exc}.{empty}")
                pre, post = [], []
            cfg = {
                "type": "table",
                "schema": target.schema_config,
                "name": target.name if target.name != n["name"] else None,
                "database": target.database,
                "tags": list(n.get("tags") or []),
                "description": n.get("description") or None,
                "columns": self._columns(n),
                "assertions": self._assertions_for(uid),
            }
            if seed.size_bytes <= seedlib.INLINE_MAX_BYTES:
                body = seedlib.inline_sql(seed)
                note = f"{len(seed.rows)} rows inlined"
            else:
                var = "seeds_gcs_path"
                self.report.settings_vars.setdefault(var, ("seed files", "gs://CHANGE-ME/seeds"))
                self.files[f"seeds/{rel}"] = csv_path.read_text()
                body = seedlib.load_data_sql(seed, f"${{dataform.projectConfig.vars.{var}}}/{rel.as_posix()}")
                cfg["type"] = "operations"
                cfg["hasOutput"] = True
                cfg.pop("assertions")
                self.report.manual(uid, f"seed is too large to inline; upload seeds/{rel} to the `{var}` GCS folder.")
                note = "LOAD DATA from GCS"
            if (pre or post) and cfg["type"] == "operations":
                body = "\n;\n".join([*pre, body, *post])
                pre, post = [], []
            self._emit(path, cfg, body, n, note, pre, post)
            self.converted.add(uid)

    # -- models -----------------------------------------------------------------

    def _model_order(self) -> list[dict]:
        models = {n["unique_id"]: n for n in self._in_scope("model")}
        deps = {uid: [d for d in n["depends_on"]["nodes"] if d in models] for uid, n in models.items()}
        order, seen = [], set()

        def visit(uid, stack=()):
            if uid in seen or uid in stack:
                return
            for d in deps[uid]:
                visit(d, stack + (uid,))
            seen.add(uid)
            order.append(models[uid])

        for uid in models:
            visit(uid)
        return order

    def _convert_models(self) -> None:
        for n in self._model_order():
            uid = n["unique_id"]
            cfg_in = n["config"]
            mat = cfg_in.get("materialized")
            rel = self._relative(n, "model")
            path = f"{self._base(n)}/{rel.with_suffix('.sqlx')}"
            if n.get("language") == "python":
                self.report.manual(uid, "Python model: Dataform has no Python models (consider a notebook action).")
                self.report.skipped["Python models"] += 1
                self._fail(uid, "Python model")
                continue
            df_type = _MATERIALIZATIONS.get(mat)
            if df_type is None:
                self.report.manual(uid, f"custom materialization `{mat}` written as a table.")
                df_type = "table"
            if mat == "ephemeral":
                self.report.warning(uid, "ephemeral model written as a view; Dataform has no ephemeral models.")

            disabled = False
            try:
                body = self._sql(n["raw_code"], n)
                pre, post = self._hooks(n)
            except Unsupported as exc:
                self.report.manual(uid, f"not converted, written disabled: {exc}")
                self._fail(uid, str(exc))
                body = (
                    f"-- dbt2dataform could not convert this model: {exc}\n"
                    "-- The original dbt SQL follows; port it by hand and remove `disabled`.\n"
                    f"{commented(n['raw_code'])}\n"
                    "SELECT 1 AS placeholder\n"
                )
                pre, post, disabled = [], [], True

            cfg = self._model_config(n, df_type, mat)
            if disabled:
                cfg["disabled"] = True
            else:
                self.converted.add(uid)
            post += self._constraint_operations(n, df_type)
            note = "" if mat == df_type else f"dbt `{mat}`"
            self._emit(path, cfg, body, n, note, pre, post)
            if self.options.contracts and (n.get("contract") or {}).get("enforced"):
                self._contract_assertion(n)

    def _model_config(self, n: dict, df_type: str, mat: str) -> dict:
        uid = n["unique_id"]
        c = n["config"]
        target = self.targets[uid]
        cfg: dict = {
            "type": df_type,
            "schema": target.schema_config,
            "name": target.name if target.name != n["name"] else None,
            "database": target.database,
            "tags": list(n.get("tags") or []),
            "description": n.get("description") or None,
            "columns": self._columns(n),
        }
        if mat == "materialized_view":
            cfg["materialized"] = True
            if c.get("on_configuration_change") not in (None, "apply"):
                self.report.info(uid, "on_configuration_change has no Dataform equivalent.")
        if df_type == "incremental":
            unique_key = c.get("unique_key")
            if unique_key:
                cfg["uniqueKey"] = [unique_key] if isinstance(unique_key, str) else list(unique_key)
            osc = c.get("on_schema_change") or "ignore"
            if osc in _ON_SCHEMA_CHANGE:
                cfg["onSchemaChange"] = _ON_SCHEMA_CHANGE[osc]
            strategy = c.get("incremental_strategy")
            if strategy == "insert_overwrite":
                cfg["incrementalStrategy"] = "INSERT_OVERWRITE"
                if c.get("partitions"):
                    self.report.warning(
                        uid,
                        "insert_overwrite with static `partitions` became Dataform's dynamic "
                        "INSERT_OVERWRITE, which replaces every partition the query returns.",
                    )
            elif strategy == "microbatch":
                self.report.manual(uid, "microbatch incremental strategy has no Dataform equivalent; written as a MERGE.")
            elif strategy not in (None, "merge", "append"):
                self.report.warning(uid, f"incremental_strategy `{strategy}` not mapped; Dataform will MERGE on uniqueKey.")
            if c.get("incremental_predicates"):
                cfg["incrementalPredicates"] = list(c["incremental_predicates"])
        if c.get("full_refresh") is False:
            cfg["protected"] = True
        bq = self._bigquery_options(c, uid)
        if bq and df_type in ("table", "incremental"):
            cfg["bigquery"] = bq
        elif bq.get("labels") and df_type == "view":
            cfg["bigquery"] = {"labels": bq["labels"]}
        if df_type in ("table", "incremental", "view"):
            cfg["assertions"] = self._assertions_for(uid)
        for key, label in _UNMAPPED_CONFIG.items():
            if c.get(key):
                self.report.manual(uid, f"{label} is not converted; Dataform has no equivalent config.")
        if c.get("grants"):
            self.report.manual(uid, "grants are not converted; add them as post_operations GRANT statements.")
        return cfg

    def _bigquery_options(self, c: dict, uid: str) -> dict:
        bq: dict = {}
        pb = c.get("partition_by")
        if isinstance(pb, dict) and pb.get("field"):
            field_, dtype = pb["field"], (pb.get("data_type") or "date").lower()
            gran = (pb.get("granularity") or "day").upper()
            if pb.get("time_ingestion_partitioning"):
                self.report.manual(uid, "ingestion-time partitioning was not converted.")
            if dtype == "date":
                bq["partitionBy"] = field_ if gran == "DAY" else f"DATE_TRUNC({field_}, {gran})"
            elif dtype == "timestamp":
                bq["partitionBy"] = f"TIMESTAMP_TRUNC({field_}, {gran})"
            elif dtype == "datetime":
                bq["partitionBy"] = f"DATETIME_TRUNC({field_}, {gran})"
            elif dtype == "int64" and pb.get("range"):
                r = pb["range"]
                bq["partitionBy"] = f"RANGE_BUCKET({field_}, GENERATE_ARRAY({r['start']}, {r['end']}, {r['interval']}))"
        if c.get("cluster_by"):
            cb = c["cluster_by"]
            bq["clusterBy"] = [cb] if isinstance(cb, str) else list(cb)
        if c.get("labels"):
            bq["labels"] = {str(k): str(v) for k, v in c["labels"].items()}
        if c.get("partition_expiration_days"):
            bq["partitionExpirationDays"] = int(c["partition_expiration_days"])
        if c.get("require_partition_filter"):
            bq["requirePartitionFilter"] = True
        return bq

    def _columns(self, node: dict) -> dict:
        """Column descriptions, and policy tags, as Dataform writes them to BigQuery."""
        out: dict = {}
        for name, col in (node.get("columns") or {}).items():
            description = (col.get("description") or "").strip()
            tags = col.get("policy_tags") or (col.get("config") or {}).get("policy_tags")
            if tags:
                out[name] = {"description": description or None, "bigqueryPolicyTags": list(tags)}
            elif description:
                out[name] = description
        return out

    def _constraint_operations(self, n: dict, df_type: str) -> list[str]:
        if df_type not in ("table", "incremental") or not (n.get("contract") or {}).get("enforced"):
            return []
        pk: list[str] = []
        for name, col in (n.get("columns") or {}).items():
            for con in col.get("constraints") or []:
                if con.get("type") == "primary_key":
                    pk.append(name)
                elif con.get("type") not in ("not_null",):
                    self.report.info(n["unique_id"], f"`{con.get('type')}` constraint on `{name}` not converted.")
        for con in n.get("constraints") or []:
            if con.get("type") == "primary_key":
                pk.extend(con.get("columns") or [])
            elif con.get("type") not in ("not_null",):
                self.report.info(n["unique_id"], f"model-level `{con.get('type')}` constraint not converted.")
        if not pk:
            return []
        cols = ", ".join(dict.fromkeys(pk))
        self.report.info(
            n["unique_id"],
            f"primary key constraint ({cols}) added as an unenforced BigQuery constraint, as dbt-bigquery does.",
        )
        return [
            "ALTER TABLE ${self()} DROP PRIMARY KEY IF EXISTS",
            f"ALTER TABLE ${{self()}} ADD PRIMARY KEY ({cols}) NOT ENFORCED",
        ]

    def _contract_assertion(self, n: dict) -> None:
        uid = n["unique_id"]
        expected = [
            (name, normalize_type(col["data_type"]))
            for name, col in (n.get("columns") or {}).items()
            if col.get("data_type")
        ]
        if not expected:
            return
        self.includes_used.add("dbt2dataform")
        rows = ",\n".join(
            f"    STRUCT({_sql_string(c)} AS column_name, {_sql_string(t)} AS data_type)" for c, t in expected
        )
        body = (
            "-- dbt enforced a contract on this model: exactly these columns, with these types.\n"
            "-- Type parameters such as NUMERIC(10, 2) are not compared.\n"
            "WITH expected AS (\n"
            f"  SELECT * FROM UNNEST([\n{rows}\n  ])\n"
            "),\n\n"
            "actual AS (\n"
            "  SELECT column_name, REGEXP_REPLACE(UPPER(data_type), r'\\([^)]*\\)', '') AS data_type\n"
            f"  FROM ${{dbt2dataform.information_schema_columns({self._ref_js(uid)})}}\n"
            ")\n\n"
            "SELECT\n"
            "  COALESCE(e.column_name, a.column_name) AS column_name,\n"
            "  e.data_type AS expected_type,\n"
            "  a.data_type AS actual_type\n"
            "FROM expected AS e\n"
            "FULL OUTER JOIN actual AS a ON LOWER(e.column_name) = LOWER(a.column_name)\n"
            "WHERE e.column_name IS NULL OR a.column_name IS NULL OR e.data_type != a.data_type\n"
        )
        name = f"{self.targets[uid].name}_contract"
        cfg = {
            "type": "assertion",
            "name": name,
            "tags": ["contract"],
            "description": f"Port of the dbt model contract on {n['name']}. dbt checks it before building; this checks the built table.",
        }
        self._emit(f"{self._base(n)}/assertions/contracts/{name}.sqlx", cfg, body, n, "dbt contract")

    # -- tests ------------------------------------------------------------------

    def _assertions_for(self, uid: str) -> dict:
        a = self.inline_assertions.get(uid)
        if not a:
            return {}
        out = {}
        if a["uniqueKeys"]:
            out["uniqueKeys"] = a["uniqueKeys"]
        if a["nonNull"]:
            out["nonNull"] = list(dict.fromkeys(a["nonNull"]))
        if a["rowConditions"]:
            out["rowConditions"] = a["rowConditions"]
        return out

    def _collect_tests(self) -> None:
        self._standalone: list[dict] = []
        self._test_stats: Counter = Counter()
        tests = self._in_scope("test")
        not_null = {
            (t.get("attached_node"), self._col(t))
            for t in tests
            if (t.get("test_metadata") or {}).get("name") == "not_null"
            and not (t["test_metadata"].get("namespace"))
            and self._inline_safe(t)
        }
        for t in tests:
            uid = t["unique_id"]
            tm = t.get("test_metadata")
            if not tm:
                self._standalone.append(t)
                continue
            attached = t.get("attached_node")
            if not attached:
                srcs = [d for d in t["depends_on"]["nodes"] if d.startswith("source.")]
                attached = srcs[0] if srcs else None
            if attached is not None and attached not in self.targets:
                self._test_stats["skipped: they test nodes disabled in dbt or left out of the conversion"] += 1
                continue
            key = (tm.get("namespace"), tm["name"])
            inline = (
                attached is not None
                and attached.split(".")[0] in ("model", "seed")
                and key in _INLINE_TESTS
                and self._inline_safe(t)
                # Dataform's uniqueKeys counts repeated NULLs as duplicates; dbt's
                # unique test ignores NULLs. Only the same when NULL is impossible.
                and (key != (None, "unique") or (attached, self._col(t)) in not_null)
            )
            if inline and self._inline_test(t, attached):
                self._test_stats["built-in assertions in the table's config"] += 1
                continue
            self._standalone.append(t)

    def _inline_safe(self, t: dict) -> bool:
        c = t.get("config") or {}
        return (
            (c.get("severity") or "ERROR").upper() == "ERROR"
            and not c.get("where")
            and _norm(c.get("fail_calc") or "count(*)") == "count(*)"
            and _norm(c.get("error_if") or "!= 0") in ("!=0", ">0")
        )

    def _col(self, t: dict) -> str | None:
        col = (t.get("test_metadata") or {}).get("kwargs", {}).get("column_name") or t.get("column_name")
        if col is None:
            return None
        if re.fullmatch(r'"[^"]+"|`[^`]+`', col):
            col = col[1:-1]
        return col

    def _inline_test(self, t: dict, target: str) -> bool:
        tm = t["test_metadata"]
        name, ns, kw = tm["name"], tm.get("namespace"), tm["kwargs"]
        bucket = self.inline_assertions[target]
        col = self._col(t)
        if name == "unique" and col:
            bucket["uniqueKeys"].append([col])
        elif name == "not_null" and col:
            bucket["nonNull"].append(col)
        elif name == "accepted_values" and col:
            bucket["rowConditions"].append(_accepted_values(col, kw))
        elif name == "unique_combination_of_columns":
            bucket["uniqueKeys"].append(list(kw["combination_of_columns"]))
        elif name == "expression_is_true" and not kw.get("condition"):
            bucket["rowConditions"].append(kw["expression"] if not col else f"{col} {kw['expression']}")
        elif name == "accepted_range" and col:
            inclusive = kw.get("inclusive", True)
            parts = []
            if kw.get("min_value") is not None:
                parts.append(f"{col} {'>=' if inclusive else '>'} {kw['min_value']}")
            if kw.get("max_value") is not None:
                parts.append(f"{col} {'<=' if inclusive else '<'} {kw['max_value']}")
            if not parts:
                return False
            bucket["rowConditions"].append(" AND ".join(parts))
        else:
            return False
        return True

    def _convert_tests(self) -> None:
        for t in self._standalone:
            uid = t["unique_id"]
            try:
                if t.get("test_metadata"):
                    sql = tidy_sql(self.renderer.render_test(t).text)
                    path = f"{self._base(t)}/assertions/generic/{t['name']}.sqlx"
                    ns = t["test_metadata"].get("namespace")
                    note = f"generic test `{(ns + '.') if ns else ''}{t['test_metadata']['name']}`"
                else:
                    sql = tidy_sql(self.renderer.render(t["raw_code"], t).text)
                    rel = self._relative(t, "test")
                    path = f"{self._base(t)}/assertions/{rel.with_suffix('.sqlx')}"
                    note = "singular test"
                body = self._finish(self._with_thresholds(sql, t))
            except Unsupported as exc:
                self.report.manual(uid, f"test not converted: {exc}")
                self._test_stats["not converted"] += 1
                self._fail(uid, str(exc))
                continue
            self._test_stats["assertion files"] += 1
            cfg = {
                "type": "assertion",
                "name": t["name"],
                "tags": list(t.get("tags") or []),
                "description": t.get("description") or None,
            }
            self._emit(path, cfg, body, t, note)
            self.converted.add(uid)
        total = sum(self._test_stats.values())
        if total:
            parts = ", ".join(f"{n} {what}" for what, n in sorted(self._test_stats.items()))
            self.report.info("tests", f"{total} dbt tests became: {parts}.")

    def _with_thresholds(self, sql: str, t: dict) -> str:
        """dbt's failure thresholds around a test's failing rows.

        dbt counts the rows a test returns (fail_calc) and compares the count
        with error_if / warn_if. A Dataform assertion fails on any row, so the
        default thresholds need nothing; others become a count that returns a
        row only when dbt would fail.
        """
        uid = t["unique_id"]
        c = t.get("config") or {}
        severity = (c.get("severity") or "ERROR").upper()
        threshold = c.get("warn_if") if severity == "WARN" else c.get("error_if")
        threshold = threshold or "!= 0"
        if severity == "WARN":
            self.report.warning(uid, "severity: warn became an assertion, which fails rather than warns.")
        if c.get("store_failures") or c.get("store_failures_as"):
            self.report.info(uid, "store_failures dropped: Dataform keeps every assertion's failing rows as a view.")
        fail_calc = c.get("fail_calc") or "count(*)"
        limit = c.get("limit")
        if _norm(fail_calc) == "count(*)" and _norm(threshold) in ("!=0", ">0"):
            return sql
        inner = sql.rstrip().rstrip(";") + (f"\nLIMIT {int(limit)}" if limit else "")
        indented = "\n".join(f"    {line}" if line else "" for line in inner.split("\n"))
        return (
            f"-- dbt fails this test when {fail_calc} {threshold}.\n"
            "SELECT *\nFROM (\n"
            f"  SELECT {fail_calc} AS failures\n"
            f"  FROM (\n{indented}\n  ) AS dbt_internal_test\n"
            ")\n"
            f"WHERE failures {threshold}\n"
        )

    # -- leftovers --------------------------------------------------------------

    def _fail(self, uid: str, why: str) -> None:
        self.failed[uid] = why

    def _flag_broken_dependencies(self) -> None:
        for uid in sorted(self.converted):
            node = self.project.nodes.get(uid) or {}
            if any(dep in self.failed for dep in node.get("depends_on", {}).get("nodes", [])):
                self.report.warning(
                    uid,
                    "depends on a model listed under Needs manual work; until that is "
                    "ported it reads whatever the table already holds, or fails if there is none.",
                )

    def _note_unconverted(self) -> None:
        for n in self._in_scope("snapshot"):
            target = self.targets[n["unique_id"]]
            path = f"{self._base(n)}/snapshots/{self._relative(n, 'snapshot').with_suffix('.sqlx')}"
            cfg = {
                "type": "declaration",
                "schema": n["schema"],
                "name": target.name,
                "database": target.database,
                "description": n.get("description") or None,
            }
            self._emit(path, cfg, "", n, "declared only")
            self.report.manual(
                n["unique_id"],
                "snapshot not converted: Dataform has no snapshots. It is declared, so "
                "models that ref it read the table dbt's snapshot maintains; port the "
                "SCD2 logic to an operations action to keep it updating.",
            )
            self.report.skipped["snapshots"] += 1
        analyses = len(self._in_scope("analysis"))
        if analyses:
            self.report.skipped["analyses"] += analyses
        for key, label in (
            ("exposures", "exposures"),
            ("metrics", "metrics"),
            ("semantic_models", "semantic models"),
            ("saved_queries", "saved queries"),
            ("unit_tests", "unit tests"),
            ("functions", "functions (UDFs)"),
        ):
            items = [v for v in (self.project.manifest.get(key) or {}).values() if self.project.in_scope(v, self.options.include_packages)]
            if not items:
                continue
            self.report.skipped[label] += len(items)
            if key == "unit_tests":
                self.report.manual("unit tests", f"{len(items)} dbt unit tests not converted; Dataform's `type: \"test\"` actions can hold them.")
            elif key == "functions":
                names = ", ".join(sorted(f.get("name", "?") for f in items))
                self.report.manual(
                    "functions",
                    f"{len(items)} dbt functions (UDFs) not converted ({names}); create them "
                    "with operations actions, and models that call them were written disabled.",
                )
        for package in sorted(self.project.package_names):
            if not self.project.in_scope({"package_name": package}, self.options.include_packages):
                continue
            yml = self.project.package_project_yml(package)
            for hook in ("on-run-start", "on-run-end"):
                if yml.get(hook):
                    self.report.manual(f"{package} dbt_project.yml", f"{hook} hooks not converted; add them as operations actions.")
        disabled = Counter(
            d.get("resource_type", "node")
            for entries in (self.project.manifest.get("disabled") or {}).values()
            for d in entries
            if self.project.in_scope(d, self.options.include_packages)
        )
        if disabled:
            parts = ", ".join(f"{n} {kind}s" for kind, n in sorted(disabled.items()))
            self.report.info("dbt", f"disabled in dbt, so not converted: {parts}.")
        governed = [
            n for n in self._in_scope("model")
            if n.get("group") or n.get("access") not in (None, "protected") or n.get("deprecation_date")
        ]
        if governed:
            self.report.info(
                "dbt",
                f"model governance (groups, access, deprecation dates) on {len(governed)} "
                "models has no Dataform equivalent and was dropped.",
            )

    # -- output -----------------------------------------------------------------

    def _base(self, node: dict) -> str:
        package = node["package_name"]
        return "definitions" if package == self.project.name else f"definitions/packages/{package}"

    def _relative(self, node: dict, kind: str) -> PurePosixPath:
        p = PurePosixPath(node["original_file_path"])
        for root in self.project.paths(node["package_name"], kind):
            try:
                return p.relative_to(root)
            except ValueError:
                continue
        return PurePosixPath(p.name)

    def _emit(
        self,
        path: str,
        cfg: dict,
        body: str,
        node: dict,
        note: str = "",
        pre: list[str] | None = None,
        post: list[str] | None = None,
    ) -> None:
        """Write one action, after merging in any matching overrides."""
        applied = self.overrides.apply(cfg, path)
        if applied:
            keys = sorted({k for s in applied for k in self.overrides.rules[s]})
            added = "overrides: " + ", ".join(f"`{k}`" for k in keys)
            note = f"{note}; {added}" if note else added
        if cfg.get("description"):
            # YAML block scalars end in a newline.
            cfg["description"] = cfg["description"].strip()
        self._write(path, sqlx_file(cfg, body, pre, post), node, cfg["type"], note)

    def _write(self, path: str, content: str, node: dict, df_type: str, note: str = "") -> None:
        if path in self.files:
            stem, n = path.removesuffix(".sqlx"), 2
            while f"{stem}_{n}.sqlx" in self.files:
                n += 1
            path = f"{stem}_{n}.sqlx"
        self.files[path] = content
        package = node.get("package_name")
        origin = node.get("original_file_path", "")
        if package and package != self.project.name:
            origin = f"{package}/{origin}"
        self.report.mappings.append(Mapping(node["unique_id"], origin, path, df_type, note))

    def _write_project_files(self) -> None:
        settings_vars = dict(self.renderer.settings_vars)
        settings_vars.update(self.report.settings_vars)
        for name, value in self.options.var_overrides.items():
            origin = settings_vars.get(name, ("--var", ""))[0]
            settings_vars[name] = (origin, value)
        self.report.settings_vars = settings_vars
        project = self.options.default_project or self.default_database or "your-gcp-project"
        if not self.options.default_project and not self.default_database:
            self.report.manual("workflow_settings.yaml", "set defaultProject to your GCP project.")
        lines = [
            f"defaultProject: {project}",
            f"defaultLocation: {self.options.default_location}",
            f"defaultDataset: {self.default_schema}",
            f"defaultAssertionDataset: {self.default_schema}_assertions",
            f"dataformCoreVersion: {self.options.core_version}",
        ]
        if settings_vars:
            lines.append("vars:")
            for name, (_origin, value) in sorted(settings_vars.items()):
                lines.append(f"  {name}: {json.dumps(value)}")
                if value.startswith(("CHANGE_ME", "gs://CHANGE-ME")):
                    self.report.manual("workflow_settings.yaml", f"set var `{name}`.")
        self.files["workflow_settings.yaml"] = "\n".join(lines) + "\n"
        for name in sorted(self.includes_used):
            self.files[f"includes/{name}.js"] = (RESOURCES / f"{name}.js").read_text()
        self.files[".gitignore"] = "node_modules/\n.df-credentials.json\n"
        self.files["CONVERSION_REPORT.md"] = self.report.render()


_TYPE_ALIASES = {
    "INT": "INT64",
    "INTEGER": "INT64",
    "BIGINT": "INT64",
    "SMALLINT": "INT64",
    "TINYINT": "INT64",
    "BYTEINT": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
    "DECIMAL": "NUMERIC",
    "BIGDECIMAL": "BIGNUMERIC",
}


def normalize_type(data_type: str) -> str:
    """A contract's data_type as INFORMATION_SCHEMA spells it, without parameters."""
    t = re.sub(r"\([^)]*\)", "", data_type).strip().upper()
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"\b[A-Z]+\b", lambda m: _TYPE_ALIASES.get(m.group(0), m.group(0)), t)


def _norm(expr: str) -> str:
    return re.sub(r"\s+", "", str(expr)).lower()


def _sql_string(value: str) -> str:
    """A BigQuery string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _option_value(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_option_value(v) for v in value) + "]"
    return _sql_string(str(value))


def _accepted_values(col: str, kw: dict) -> str:
    quote = kw.get("quote", True)
    values = ", ".join(_sql_string(str(v)) if quote else str(v) for v in kw["values"])
    return f"{col} IN ({values})"


def write_output(files: dict[str, str], out_dir: Path, force: bool) -> None:
    """Write the project, replacing files from a previous run and nothing else."""
    out_dir.mkdir(parents=True, exist_ok=True)
    state = out_dir / STATE_FILE
    previous: list[str] = []
    if state.exists():
        previous = json.loads(state.read_text()).get("files", [])
        for rel in previous:
            (out_dir / rel).unlink(missing_ok=True)
    elif any(out_dir.iterdir()) and not force:
        raise FileExistsError(
            f"{out_dir} is not empty and was not written by dbt2dataform; pass --force to write into it"
        )
    written = []
    for rel, content in files.items():
        path = out_dir / rel
        if rel in _OPTIONAL_FILES and path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        written.append(rel)
    state.write_text(json.dumps({"files": sorted(written)}, indent=2) + "\n")
    # Directories that removing the previous run's files left empty.
    emptied = {p for rel in previous for p in (out_dir / rel).parents if out_dir in p.parents}
    for d in sorted(emptied, key=lambda p: -len(p.parts)):
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()


# Written only when the output directory has none of its own, so that a
# generated project can live at the root of a repository that keeps its own.
_OPTIONAL_FILES = {".gitignore"}
