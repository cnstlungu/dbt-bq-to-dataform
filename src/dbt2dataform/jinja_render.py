"""Renders dbt Jinja the way dbt does, leaving tokens where Dataform needs JavaScript.

Every macro in the manifest is available: the project's own, its packages',
and dbt's (dbt-bigquery's implementations ahead of dbt-core's). Names resolve
by dbt's rules, adapter.dispatch included, so a macro call renders exactly the
SQL dbt-bigquery would compile it to.

What differs from dbt is the context around the macros. ref(), source(),
this and project vars render as tokens (tokens.py) that later become
`${ref()}`, `${self()}` and `${dataform.projectConfig.vars.x}`.
is_incremental() returns whichever branch the caller asked for; the caller
renders twice and diffs. Anything that needs a live warehouse (run_query,
adapter introspection, statement blocks) raises Unsupported, and the node is
flagged instead of silently mistranslated.
"""

from __future__ import annotations

import ast
import datetime
import hashlib
import itertools
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from types import SimpleNamespace

import jinja2
import jinja2.ext
import yaml

from .dbt_project import DbtProject
from .errors import Unsupported
from .report import Report
from .tokens import TokenRegistry

try:  # dbt ships pytz in its context's `modules`
    import pytz
except ImportError:  # pragma: no cover
    pytz = None

__all__ = ["Renderer", "Rendered", "Unsupported"]

# The order dbt-bigquery dispatches in, and where dbt's own macros live.
ADAPTER_PREFIXES = ("bigquery", "default")
INTERNAL_PACKAGES = ("dbt_bigquery", "dbt")
# Context members that stand in for dbt-core macros of the same name.
_REPLACES_INTERNAL = {"is_incremental", "run_query", "statement"}

_MISSING = object()
_TEST_BLOCK = re.compile(r"\{%(-?)\s*test\s+(\w+)\s*\(")
_END_TEST = re.compile(r"\{%(-?)\s*endtest\s*(-?)%\}")
_MATERIALIZATION = re.compile(r"\{%-?\s*materialization\b")
# dbt renders a test argument that looks like one of these calls as Jinja.
_LOOKS_LIKE_FUNC = re.compile(r"^\s*(env_var|ref|var|source|doc)\s*\(.+\)\s*$", re.S)
_SINGLE_EXPRESSION = re.compile(r"^\s*\{\{(.*)\}\}\s*$", re.S)


class _MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


class _Undefined(jinja2.StrictUndefined):
    """Fails loudly, naming what was missing, the first time it is used."""


def _var_name(name: str) -> str:
    return re.sub(r"\W", "_", name)


class _VarStr(str):
    df_var: str


class _VarInt(int):
    df_var: str


class _VarFloat(float):
    df_var: str


def _wrap_var(dataform_name: str, value):
    """A var's value that also remembers where it came from.

    Used in Jinja logic it behaves as the plain value, evaluated now. Printed
    with {{ }} it becomes a token for `dataform.projectConfig.vars.<name>`, so
    the Dataform project stays configurable the way the dbt one was.
    """
    for base, cls in ((bool, None), (str, _VarStr), (int, _VarInt), (float, _VarFloat)):
        if isinstance(value, base):
            if cls is None:
                return value
            wrapped = cls(value)
            wrapped.df_var = dataform_name
            return wrapped
    return value


class _RunStartedAt(datetime.datetime):
    """run_started_at. Printed, it becomes the time Dataform compiles the project."""

    df_js = "new Date().toISOString()"

    @classmethod
    def of(cls, value: datetime.datetime) -> "_RunStartedAt":
        return cls(*value.timetuple()[:6], value.microsecond, tzinfo=value.tzinfo)

    def astimezone(self, tz=None):
        return _RunStartedAt.of(super().astimezone(tz))

    def replace(self, *args, **kwargs):
        return _RunStartedAt.of(super().replace(*args, **kwargs))


class _Unavailable:
    """A context value with no Dataform equivalent: any use flags the node."""

    def __init__(self, why: str):
        self._why = why

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        raise Unsupported(self._why)

    def __call__(self, *_args, **_kwargs):
        raise Unsupported(self._why)

    def __str__(self):
        raise Unsupported(self._why)


class Relation(str):
    """What ref(), source() and this return: a token that also answers .identifier etc."""

    def __new__(cls, token: str, database: str | None, schema: str | None, identifier: str, type_: str | None = None):
        obj = str.__new__(cls, token)
        obj.database = obj.project = database
        obj.schema = obj.dataset = schema
        obj.identifier = obj.name = obj.table = identifier
        obj.type = type_
        obj.is_table = type_ == "table"
        obj.is_view = type_ == "view"
        obj.is_materialized_view = type_ == "materialized_view"
        obj.is_cte = False
        return obj

    def render(self) -> str:
        return str(self)

    def include(self, **_kwargs) -> "Relation":
        return self

    def quote(self, **_kwargs) -> "Relation":
        return self

    def incorporate(self, **_kwargs) -> "Relation":
        return self


class _Config:
    def __init__(self, node_config: dict):
        self._cfg = node_config

    def __call__(self, *_args, **_kwargs) -> str:
        return ""

    def get(self, key, default=None):
        value = self._cfg.get(key)
        return default if value is None else value

    def require(self, key):
        if self._cfg.get(key) is None:
            raise Unsupported(f"config '{key}' is required but not set")
        return self._cfg[key]

    def meta_get(self, key, default=None):
        return (self._cfg.get("meta") or {}).get(key, default)

    def meta_require(self, key):
        meta = self._cfg.get("meta") or {}
        if key not in meta:
            raise Unsupported(f"config meta '{key}' is required but not set")
        return meta[key]

    def set(self, *_args, **_kwargs) -> str:
        return ""


class _Recorder:
    """An object whose attribute reads are recorded as conversion warnings."""

    def __init__(self, label: str, values: dict, on_read):
        object.__setattr__(self, "_label", label)
        object.__setattr__(self, "_values", values)
        object.__setattr__(self, "_on_read", on_read)

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        self._on_read(f"{self._label}.{item}")
        if item in self._values:
            return self._values[item]
        raise Unsupported(f"{self._label}.{item} has no Dataform equivalent")


class _Adapter:
    def __init__(self, scope: "_Scope"):
        self._scope = scope

    def dispatch(self, macro_name, macro_namespace=None, packages=None):
        return self._scope.dispatch(macro_name, macro_namespace)

    def quote(self, identifier) -> str:
        return f"`{identifier}`"

    def type(self) -> str:
        return "bigquery"

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        raise Unsupported(
            f"adapter.{item}() needs a live warehouse connection, which a static "
            "conversion does not have"
        )


@dataclass
class Rendered:
    text: str
    used_is_incremental: bool


class MacroLibrary:
    """Every macro in the manifest by package, compiled the first time it is called."""

    def __init__(self, macros: dict, env: jinja2.Environment):
        self.env = env
        self.by_package: dict[str, dict[str, dict]] = defaultdict(dict)
        for m in macros.values():
            self.by_package[m["package_name"]][m["name"]] = m
        self.internal = [p for p in INTERNAL_PACKAGES if p in self.by_package]
        self._templates: dict[str, jinja2.Template | Unsupported] = {}

    def find(self, package: str | None, name: str) -> dict | None:
        return self.by_package.get(package, {}).get(name) if package else None

    def find_internal(self, name: str) -> dict | None:
        for package in self.internal:
            macro = self.by_package[package].get(name)
            if macro:
                return macro
        return None

    def template(self, macro: dict) -> jinja2.Template:
        uid = macro["unique_id"]
        if uid not in self._templates:
            sql = macro["macro_sql"]
            if _MATERIALIZATION.search(sql):
                result = Unsupported(f"{uid} is a materialization, which Dataform has no equivalent of")
            else:
                # {% test %} blocks are macros named test_<name>, as dbt reads them.
                sql = _END_TEST.sub(r"{%\1 endmacro \2%}", _TEST_BLOCK.sub(r"{%\1 macro test_\2(", sql))
                try:
                    result = self.env.from_string(sql)
                except jinja2.exceptions.TemplateSyntaxError as exc:
                    result = Unsupported(f"{uid} uses Jinja syntax plain Jinja does not have: {exc.message}")
            self._templates[uid] = result
        result = self._templates[uid]
        if isinstance(result, Unsupported):
            raise result
        return result


class _MacroCaller:
    def __init__(self, scope: "_Scope", macro: dict):
        self._scope = scope
        self._macro = macro
        self._fn = None

    def __call__(self, *args, **kwargs):
        if self._fn is None:
            template = self._scope.library.template(self._macro)
            ctx = template.new_context(vars=self._scope.context, shared=True)
            for _ in template.root_render_func(ctx):
                pass
            # Read the context, not the module: modules do not export _private macros.
            self._fn = ctx.vars.get(self._macro["name"])
            if self._fn is None:
                raise Unsupported(f"{self._macro['unique_id']} did not define a macro")
        try:
            return self._fn(*args, **kwargs)
        except _MacroReturn as ret:
            return ret.value


class _PackageNamespace:
    """`dbt_utils.x`, `dbt.x`, `my_project.x`."""

    def __init__(self, scope: "_Scope", package: str):
        self._scope = scope
        self._package = package

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        macro = self._scope.get_from_package(self._package, name)
        if macro is None:
            raise Unsupported(f"there is no macro `{self._package}.{name}`")
        return macro


class _LazyContext(dict):
    """The Jinja globals of one render, resolved on first use like dbt's ChainMap."""

    def __init__(self, scope: "_Scope"):
        super().__init__()
        self._scope = scope
        self._cache: dict = {}

    def _lookup(self, key):
        if key not in self._cache:
            self._cache[key] = self._scope.resolve(key)
        return self._cache[key]

    def __contains__(self, key):
        return self._lookup(key) is not _MISSING

    def __getitem__(self, key):
        value = self._lookup(key)
        if value is _MISSING:
            raise KeyError(key)
        return value

    def get(self, key, default=None):
        value = self._lookup(key)
        return default if value is _MISSING else value


class _Scope:
    """dbt's MacroNamespace for one render: how a bare name resolves.

    In order: the node's own package, the root project, package namespaces,
    the `dbt` namespace, the context members, then dbt's own macros by bare
    name. dbt puts its macros ahead of the members; the members go first
    here only so the stand-ins in _REPLACES_INTERNAL win over dbt-core's
    real is_incremental() and friends.
    """

    def __init__(self, renderer: "Renderer", members: dict, package: str):
        self.renderer = renderer
        self.library = renderer.library
        self.members = members
        self.package = package
        self.root = renderer.project.name
        self.context = _LazyContext(self)
        self._callers: dict[str, _MacroCaller] = {}

    def caller(self, macro: dict):
        uid = macro["unique_id"]
        if uid not in self._callers:
            real = _MacroCaller(self, macro)
            stand_in = _STAND_INS.get(macro["name"]) if macro["package_name"] in _STAND_IN_PACKAGES else None
            self._callers[uid] = (lambda *a, **k: stand_in(self, real, *a, **k)) if stand_in else real
        return self._callers[uid]

    def resolve(self, name: str):
        lib = self.library
        for package in (self.package, self.root):
            macro = lib.find(package, name)
            if macro:
                return self.caller(macro)
        if name in lib.by_package and name not in lib.internal:
            return _PackageNamespace(self, name)
        if name == "dbt":
            return _PackageNamespace(self, "dbt")
        if name in self.members:
            self.renderer.on_member(name, self)
            return self.members[name]
        macro = lib.find_internal(name)
        if macro:
            return self.caller(macro)
        return _MISSING

    def get_from_package(self, package: str | None, name: str):
        lib = self.library
        if package is None:
            for pkg in (self.package, self.root):
                macro = lib.find(pkg, name)
                if macro:
                    return self.caller(macro)
            if name in _REPLACES_INTERNAL:
                return self.members[name]
            macro = lib.find_internal(name)
            return self.caller(macro) if macro else None
        if package == "dbt" or package in lib.internal:
            if name in _REPLACES_INTERNAL:
                return self.members[name]
            macro = lib.find_internal(name)
        else:
            macro = lib.find(package, name)
        return self.caller(macro) if macro else None

    def dispatch(self, macro_name: str, macro_namespace: str | None):
        """adapter.dispatch, by dbt's search order for the bigquery adapter."""
        if macro_namespace is None:
            packages: list[str | None] = [None]
        else:
            packages = self.renderer.dispatch_order.get(macro_namespace) or (
                [self.root, macro_namespace]
                if macro_namespace in self.library.by_package or macro_namespace == "dbt"
                else [None]
            )
        for package in packages:
            for prefix in ADAPTER_PREFIXES:
                macro = self.get_from_package(package, f"{prefix}__{macro_name}")
                if macro is not None:
                    return macro
        raise Unsupported(f"adapter.dispatch found no implementation of `{macro_name}`")


class _SqlNumber:
    """A number dbt would fetch with a query, kept as the SQL that computes it."""

    def __init__(self, sql: str):
        self.sql = sql

    def __str__(self):
        return f"({self.sql})"


def _intervals_between(scope: "_Scope", _real, start_date, end_date, datepart):
    # dbt runs `select datediff(...)` to size a date spine. Keep that SQL, and
    # let the series below compute the same count in the query itself.
    datediff = scope.get_from_package("dbt", "datediff")
    scope.renderer.report.info(
        scope.members["model"].get("unique_id", "?"),
        "a date spine counts its intervals with a warehouse query in dbt; here "
        "GENERATE_ARRAY computes the same count inside the SQL.",
    )
    return _SqlNumber(str(datediff(start_date, end_date, datepart)).strip())


def _generate_series(_scope: "_Scope", real, upper_bound):
    if isinstance(upper_bound, _SqlNumber):
        # The rows dbt's generate_series(n) returns: generated_number 1..n.
        return (
            "select generated_number\n"
            f"from unnest(generate_array(1, {upper_bound.sql})) as generated_number"
        )
    return real(upper_bound)


# Macros that only need a warehouse to fetch something the SQL can compute
# itself, replaced wherever dbt or dbt_utils dispatches to them.
_STAND_INS = {
    "default__get_intervals_between": _intervals_between,
    "default__generate_series": _generate_series,
}
_STAND_IN_PACKAGES = {"dbt", "dbt_utils"}


class Renderer:
    def __init__(self, project: DbtProject, registry: TokenRegistry, report: Report, target_name: str | None = None):
        self.project = project
        self.registry = registry
        self.report = report
        self.env = jinja2.Environment(
            extensions=[jinja2.ext.do, jinja2.ext.loopcontrols],
            undefined=_Undefined,
            finalize=self._finalize,
            keep_trailing_newline=True,
        )
        self.env.filters.update(
            as_bool=_as_native, as_native=_as_native, as_number=_as_native, as_text=str
        )
        self.library = MacroLibrary(project.macros, self.env)
        self.dispatch_order = {
            d["macro_namespace"]: d.get("search_order") or []
            for d in project.project_yml.get("dispatch") or []
            if isinstance(d, dict) and d.get("macro_namespace")
        }
        self.target_name = target_name or "default"
        self.run_started_at = _RunStartedAt.of(datetime.datetime.now(datetime.timezone.utc))
        self._templates: dict[str, jinja2.Template] = {}
        self._static = self._static_members()
        # dataform var name -> (origin description, value to write in settings)
        self.settings_vars: dict[str, tuple[str, str]] = {}

    # -- public -----------------------------------------------------------------

    def render(self, raw: str, node: dict, incremental: bool = False, extra: dict | None = None) -> Rendered:
        state = {"used_is_incremental": False}
        scope = self._scope(node, incremental, state, extra)
        return Rendered(self._render_in(scope, raw), state["used_is_incremental"])

    def render_test(self, node: dict) -> Rendered:
        """A generic test's SQL, by calling its test macro as dbt does."""
        state = {"used_is_incremental": False}
        scope = self._scope(node, False, state, {})
        kwargs = _deep_map(lambda v, path: self._test_kwarg(v, path, scope), node["test_metadata"]["kwargs"])
        scope.members["_dbt_generic_test_kwargs"] = kwargs
        return Rendered(self._render_in(scope, node["raw_code"]), False)

    def on_member(self, name: str, scope: _Scope) -> None:
        if name == "run_started_at":
            self.report.warning(
                scope.members["model"].get("unique_id", "?"),
                "run_started_at, printed as is, became the time Dataform compiles the "
                "project; anything computed from it was fixed at conversion time.",
            )

    # -- rendering --------------------------------------------------------------

    def _render_in(self, scope: _Scope, raw: str) -> str:
        template = self._templates.get(raw)
        if template is None:
            try:
                template = self.env.from_string(raw)
            except jinja2.exceptions.TemplateSyntaxError as exc:
                raise Unsupported(f"Jinja syntax plain Jinja does not have: {exc}") from exc
            self._templates[raw] = template
        ctx = template.new_context(vars=scope.context, shared=True)
        try:
            return self.env.concat(template.root_render_func(ctx))
        except jinja2.exceptions.UndefinedError as exc:
            raise Unsupported(f"Jinja could not resolve a name: {exc.message}") from exc
        except _MacroReturn as ret:
            return str(ret.value)

    def _native(self, text: str, scope: _Scope):
        """Render like dbt's native renderer: a lone expression keeps its type."""
        single = _SINGLE_EXPRESSION.match(text)
        if single:
            # One list per scope: the context caches what a name resolved to.
            holder: list = scope.members.setdefault("__dbt2dataform_native__", [])
            holder.clear()
            self._render_in(scope, "{%- do __dbt2dataform_native__.append(" + single.group(1) + ") -%}")
            return holder[0]
        return _as_native(self._render_in(scope, text))

    def _test_kwarg(self, value, path, scope: _Scope):
        if not isinstance(value, str) or path == ("column_name",):
            return value
        if _LOOKS_LIKE_FUNC.match(value):
            value = "{{ " + value + " }}"
        return self._native(value, scope)

    def _finalize(self, value):
        if isinstance(value, _Unavailable):
            str(value)  # raises Unsupported
        df_var = getattr(value, "df_var", None)
        if df_var is not None:
            return self.registry.token("var", df_var)
        df_js = getattr(value, "df_js", None)
        if df_js is not None:
            return self.registry.token("js", df_js)
        return value

    # -- context ----------------------------------------------------------------

    def _static_members(self) -> dict:
        def _return(value):
            raise _MacroReturn(value)

        def log(*_args, **_kwargs):
            return ""

        def raise_compiler_error(message="", *_a, **_k):
            raise Unsupported(f"the dbt code raises a compiler error: {message}")

        def try_or_compiler_error(message, func, *args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Unsupported:
                raise
            except Exception as exc:
                raise Unsupported(f"{message}: {exc}") from exc

        modules = {"datetime": datetime, "re": re, "itertools": itertools}
        if pytz is not None:
            modules["pytz"] = pytz
        warehouse = "needs a live warehouse connection, which a static conversion does not have"
        members = dict(self.env.globals)
        members.update(
            execute=True,
            flags=SimpleNamespace(
                FULL_REFRESH=False, WHICH="run", STORE_FAILURES=False, EMPTY=False,
                DEBUG=False, WARN_ERROR=False, USE_COLORS=False,
            ),
            run_query=_Unavailable(f"run_query() {warehouse}"),
            statement=_Unavailable(f"statement blocks {warehouse}"),
            load_result=_Unavailable(f"load_result() {warehouse}"),
            store_result=_Unavailable(f"store_result() {warehouse}"),
            store_raw_result=_Unavailable(f"store_raw_result() {warehouse}"),
            write=_Unavailable("write() writes files during a dbt run"),
            submit_python_job=_Unavailable("Python models have no Dataform equivalent"),
            api=_api(),
            invocation_id=_Unavailable("invocation_id identifies a dbt run; Dataform has no equivalent"),
            run_started_at=self.run_started_at,
            graph=self._graph(),
            log=log,
            print=log,
            exceptions=SimpleNamespace(
                raise_compiler_error=raise_compiler_error,
                raise_not_implemented=raise_compiler_error,
                warn=lambda *_a, **_k: "",
            ),
            modules=SimpleNamespace(**modules),
            tojson=json.dumps,
            fromjson=json.loads,
            toyaml=yaml.safe_dump,
            fromyaml=yaml.safe_load,
            zip=zip,
            zip_strict=lambda *a: list(zip(*a, strict=True)),
            set=set,
            set_strict=set,
            local_md5=lambda s: hashlib.md5(str(s).encode()).hexdigest(),
            try_or_compiler_error=try_or_compiler_error,
            project_name=self.project.name,
            dbt_version=self.project.dbt_version,
            invocation_args_dict={},
            dbt_metadata_envs={},
            selected_resources=[],
            doc=self._doc,
        )
        members["return"] = _return
        return members

    def _graph(self) -> dict:
        m = self.project.manifest
        return {
            "nodes": m.get("nodes", {}),
            "sources": m.get("sources", {}),
            "exposures": m.get("exposures", {}),
            "metrics": m.get("metrics", {}),
            "groups": m.get("groups", {}),
            "semantic_models": m.get("semantic_models", {}),
            "saved_queries": m.get("saved_queries", {}),
        }

    def _doc(self, *args):
        name = args[-1]
        for d in self.project.manifest.get("docs", {}).values():
            if d["name"] == name and (len(args) == 1 or d["package_name"] == args[0]):
                return d["block_contents"]
        raise Unsupported(f"doc('{name}') is not defined")

    def _scope(self, node: dict, incremental: bool, state: dict, extra: dict | None) -> _Scope:
        subject = node.get("unique_id", "?")
        registry = self.registry
        package = node.get("package_name") or self.project.name
        project_vars = self.project.vars_for(package)

        def is_incremental() -> bool:
            state["used_is_incremental"] = True
            return incremental and node.get("config", {}).get("materialized") == "incremental"

        def ref(*args, **kwargs):
            version = kwargs.get("v", kwargs.get("version"))
            if len(args) == 1:
                ref_package, name = None, args[0]
            elif len(args) == 2:
                ref_package, name = args
            else:
                raise Unsupported(f"ref{args} has an unexpected shape")
            target = self._resolve_ref(node, ref_package, name, version)
            materialized = (target.get("config") or {}).get("materialized")
            return Relation(
                registry.token("ref", target["unique_id"]),
                target.get("database"),
                target.get("schema"),
                target.get("alias") or target["name"],
                "view" if materialized in ("view", "ephemeral") else materialized and "table",
            )

        def source(source_name, table_name):
            target = self._resolve_source(node, source_name, table_name)
            return Relation(
                registry.token("ref", target["unique_id"]),
                target.get("database"),
                target.get("schema"),
                target.get("identifier") or target["name"],
                "table",
            )

        def var(name, default=_MISSING):
            if name in project_vars:
                value = project_vars[name]
            elif default is not _MISSING:
                value = default
            else:
                raise Unsupported(f"var('{name}') is not defined and has no default")
            if isinstance(value, str) and ("{{" in value or "{%" in value):
                # dbt renders a var's Jinja when the var is read.
                self.report.info(f"var {name}", "holds Jinja, which was rendered at conversion time.")
                return self._render_in(scope, value)
            if value is None or isinstance(value, (dict, list, bool)):
                if value is not None:
                    self.report.info(
                        f"var {name}",
                        "is a boolean, list or mapping, so its value was inlined at "
                        "conversion time; Dataform vars are strings only.",
                    )
                return value
            df_name = _var_name(name)
            self.settings_vars.setdefault(df_name, (f"dbt var `{name}`", str(value)))
            return _wrap_var(df_name, value)

        def env_var(name, default=None):
            df_name = _var_name(name).lower()
            shown = default if default is not None else f"CHANGE_ME_{name}"
            self.settings_vars.setdefault(df_name, (f"env var `{name}`", str(shown)))
            self.report.warning(
                subject,
                f"env_var('{name}') became Dataform var `{df_name}`; Dataform reads "
                "vars from workflow_settings.yaml or `--vars`, not the environment.",
            )
            return _wrap_var(df_name, str(default) if default is not None else "")

        def on_read(what):
            if what == "target.type":
                return  # always bigquery, in dbt as in Dataform
            self.report.warning(
                subject,
                f"`{what}` was evaluated at conversion time; dbt evaluates it per "
                "target, Dataform has no equivalent.",
            )

        cfg = node.get("config", {})
        config = _Config(cfg)
        members = dict(self._static)
        members.update(
            ref=ref,
            source=source,
            var=var,
            env_var=env_var,
            is_incremental=is_incremental,
            this=Relation(
                registry.token("self"),
                node.get("database"),
                node.get("schema"),
                node.get("alias") or node.get("name", ""),
            ),
            config=config,
            builtins={"ref": ref, "source": source, "config": config},
            model=node,
            database=node.get("database"),
            schema=node.get("schema"),
            target=_Recorder(
                "target",
                {
                    "name": self.target_name,
                    "type": "bigquery",
                    "schema": node.get("schema"),
                    "dataset": node.get("schema"),
                    "database": node.get("database"),
                    "project": node.get("database"),
                    "threads": 1,
                },
                on_read,
            ),
        )
        members.update(extra or {})
        scope = _Scope(self, members, package)
        members["adapter"] = _Adapter(scope)
        members["context"] = scope.context
        members["render"] = lambda text: self._render_in(scope, text)
        return scope

    # -- lookups ----------------------------------------------------------------

    def _resolve_ref(self, node: dict, package, name, version) -> dict:
        candidates = [
            n
            for n in self.project.nodes.values()
            if n["resource_type"] in ("model", "seed", "snapshot")
            and n["name"] == name
            and (package is None or n["package_name"] == package)
            and (version is None or str(n.get("version")) == str(version))
        ]
        if len(candidates) > 1:
            depends = set(node.get("depends_on", {}).get("nodes", []))
            narrowed = [c for c in candidates if c["unique_id"] in depends]
            latest = [c for c in candidates if c.get("version") == c.get("latest_version")]
            candidates = narrowed or latest or candidates
        if not candidates:
            raise Unsupported(f"ref('{name}') does not resolve to an enabled model, seed or snapshot")
        return candidates[0]

    def _resolve_source(self, node: dict, source_name, table_name) -> dict:
        matches = [
            s
            for s in self.project.sources.values()
            if s["source_name"] == source_name and s["name"] == table_name
        ]
        if not matches:
            raise Unsupported(f"source('{source_name}', '{table_name}') is not defined")
        depends = set(node.get("depends_on", {}).get("nodes", []))
        return next((s for s in matches if s["unique_id"] in depends), matches[0])


def _api() -> SimpleNamespace:
    """`api.Column` and `api.Relation`: dbt-bigquery's own when importable."""
    try:
        from dbt.adapters.bigquery.column import BigQueryColumn
        from dbt.adapters.bigquery.relation import BigQueryRelation

        return SimpleNamespace(Column=BigQueryColumn, Relation=BigQueryRelation)
    except Exception:
        return SimpleNamespace(Column=_Column, Relation=_RelationFactory)


class _Column:
    """The static parts of dbt-bigquery's BigQueryColumn."""

    TYPE_LABELS = {"TEXT": "STRING", "FLOAT": "FLOAT64", "INTEGER": "INT64"}

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return cls.TYPE_LABELS.get(dtype.upper(), dtype)

    @classmethod
    def numeric_type(cls, dtype: str, precision, scale) -> str:
        return dtype if precision is None or scale is None else f"{dtype}({precision},{scale})"

    @classmethod
    def string_type(cls, size: int) -> str:
        return "STRING"


class _RelationFactory:
    @staticmethod
    def create(database=None, schema=None, identifier=None, **_kwargs) -> str:
        return ".".join(f"`{part}`" for part in (database, schema, identifier) if part)


def _as_native(value):
    """dbt's native rendering: a string that reads as a Python literal becomes one."""
    if not isinstance(value, str):
        return value
    try:
        parsed = ast.literal_eval(value.strip())
    except (ValueError, SyntaxError, MemoryError, RecursionError):
        return value
    return value if isinstance(parsed, str) else parsed


def _deep_map(fn, value, path=()):
    if isinstance(value, dict):
        return {k: _deep_map(fn, v, path + (k,)) for k, v in value.items()}
    if isinstance(value, list):
        return [_deep_map(fn, v, path + (i,)) for i, v in enumerate(value)]
    return fn(value, path)
