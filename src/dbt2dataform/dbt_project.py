"""Loads a dbt-bigquery project: its parsed manifest plus the raw YAML dbt renders away."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

import yaml

from .errors import ProjectError

_MISSING_ENV = re.compile(r"Env var required but not provided: '([^']+)'")
# Value given to an env var dbt insists on at parse time. Anything rendered
# from it is re-derived from the raw YAML/SQL, so the value never reaches the
# output; the marker just makes a leak easy to spot.
ENV_PLACEHOLDER = "__dbt2dataform_env_{}__"
# The dbt-core releases whose manifest (v12) this converter is tested against.
SUPPORTED_DBT = ((1, 8), (1, 12))
INTERNAL_PACKAGES = ("dbt", "dbt_bigquery")
_PACKAGE_FILES = ("packages.yml", "dependencies.yml")

__all__ = ["DbtProject", "ProjectError", "load_project"]


@dataclass(eq=False)
class DbtProject:
    root: Path
    manifest: dict
    project_yml: dict
    name: str
    adapter_type: str
    parse_env: dict[str, str] = field(default_factory=dict)
    # Where `dbt deps` installed packages: inside the parse copy when we parsed.
    packages_dir: Path | None = None
    work_dir: Path | None = None
    # dbt's `target`: name, schema/dataset, database/project, location, threads
    target: dict = field(default_factory=dict)
    _package_yml: dict = field(default_factory=dict)
    _vars: dict = field(default_factory=dict)

    @property
    def nodes(self) -> dict:
        return self.manifest["nodes"]

    @property
    def sources(self) -> dict:
        return self.manifest["sources"]

    @property
    def macros(self) -> dict:
        return self.manifest["macros"]

    @property
    def dbt_version(self) -> str:
        return self.manifest.get("metadata", {}).get("dbt_version") or "unknown"

    @cached_property
    def package_names(self) -> set[str]:
        names = {n["package_name"] for n in self.nodes.values()}
        names |= {s["package_name"] for s in self.sources.values()}
        names |= {m["package_name"] for m in self.macros.values()}
        return names

    def in_scope(self, node: dict, include_packages: bool) -> bool:
        package = node["package_name"]
        return package == self.name or (include_packages and package not in INTERNAL_PACKAGES)

    def of_type(self, resource_type: str, include_packages: bool) -> list[dict]:
        """Enabled nodes of one type that the conversion covers, root project first."""
        return sorted(
            (
                n
                for n in self.nodes.values()
                if n["resource_type"] == resource_type and self.in_scope(n, include_packages)
            ),
            key=lambda n: (n["package_name"] != self.name, n["unique_id"]),
        )

    def sources_in_scope(self, include_packages: bool) -> list[dict]:
        return sorted(
            (s for s in self.sources.values() if self.in_scope(s, include_packages)),
            key=lambda s: (s["package_name"] != self.name, s["unique_id"]),
        )

    def package_dir(self, package: str) -> Path | None:
        if package == self.name:
            return self.root
        if self.packages_dir is not None:
            path = self.packages_dir / package
            if path.exists():
                return path.resolve()
        return None

    def file_of(self, node: dict) -> Path | None:
        base = self.package_dir(node["package_name"])
        return base / node["original_file_path"] if base else None

    def package_project_yml(self, package: str) -> dict:
        if package == self.name:
            return self.project_yml
        if package not in self._package_yml:
            base = self.package_dir(package)
            path = base / "dbt_project.yml" if base else None
            doc = yaml.safe_load(path.read_text()) if path and path.exists() else None
            self._package_yml[package] = doc or {}
        return self._package_yml[package]

    def vars_for(self, package: str) -> dict:
        """The vars a node in `package` sees, by dbt's precedence.

        The root project's vars beat the package's own; within each file, vars
        scoped to the package beat the global ones.
        """
        if package not in self._vars:
            names = self.package_names

            def layer(raw: dict) -> dict:
                merged = {k: v for k, v in raw.items() if k not in names}
                scoped = raw.get(package)
                if isinstance(scoped, dict):
                    merged.update(scoped)
                return merged

            merged: dict = {}
            if package != self.name:
                merged.update(layer(self.package_project_yml(package).get("vars") or {}))
            merged.update(layer(self.project_yml.get("vars") or {}))
            self._vars[package] = merged
        return self._vars[package]

    def paths(self, package: str, kind: str) -> list[str]:
        yml = self.package_project_yml(package)
        defaults = {
            "model": (["model-paths", "source-paths"], ["models"]),
            "seed": (["seed-paths", "data-paths"], ["seeds"]),
            "test": (["test-paths"], ["tests"]),
            "snapshot": (["snapshot-paths"], ["snapshots"]),
        }
        keys, default = defaults[kind]
        for key in keys:
            if yml.get(key):
                return yml[key]
        return default

    def raw_source_entry(self, source: dict) -> tuple[dict, dict]:
        """The source and table blocks for `source` as written in YAML, Jinja intact.

        dbt renders env_var() and friends into the manifest at parse time,
        which would bake a developer's local values into the output.
        """
        path = self.file_of(source)
        if path is None or not path.exists():
            return {}, {}
        doc = yaml.safe_load(path.read_text()) or {}
        for src in doc.get("sources") or []:
            if src.get("name") != source["source_name"]:
                continue
            for table in src.get("tables") or []:
                if table.get("name") == source["name"]:
                    return src, table
            return src, {}
        return {}, {}

    def close(self) -> None:
        """Remove the parse copy."""
        if self.work_dir is not None:
            shutil.rmtree(self.work_dir, ignore_errors=True)
            self.work_dir = None


def load_project(
    project_dir: Path,
    manifest_path: Path | None = None,
    profiles_dir: Path | None = None,
    target: str | None = None,
    dbt_command: str | None = None,
    profile: str | None = None,
) -> DbtProject:
    project_dir = project_dir.resolve()
    project_file = project_dir / "dbt_project.yml"
    if not project_file.exists():
        raise ProjectError(f"{project_dir} has no dbt_project.yml")
    project_yml = yaml.safe_load(project_file.read_text()) or {}
    install_path = project_yml.get("packages-install-path") or "dbt_packages"

    profiles_dir = _profiles_dir(project_dir, profiles_dir)
    parse_env: dict[str, str] = {}
    work = None
    if manifest_path is None:
        manifest, parse_env, work = _parse(project_dir, profiles_dir, target, dbt_command, profile)
        packages_dir = work / project_dir.name / install_path
    else:
        manifest = json.loads(Path(manifest_path).read_text())
        packages_dir = project_dir / install_path

    meta = manifest.get("metadata", {})
    adapter = meta.get("adapter_type") or "unknown"
    if adapter != "bigquery":
        if work is not None:
            shutil.rmtree(work, ignore_errors=True)
        raise ProjectError(
            f"this is a dbt-{adapter} project. dbt2dataform converts dbt-bigquery "
            "projects only: Dataform runs on BigQuery, and translating SQL between "
            "warehouses is a different job. Move the project to dbt-bigquery first "
            "(BigQuery's SQL translation service can help), then convert it."
        )
    return DbtProject(
        root=project_dir,
        manifest=manifest,
        project_yml=project_yml,
        name=meta.get("project_name") or project_yml["name"],
        adapter_type=adapter,
        parse_env=parse_env,
        packages_dir=packages_dir,
        work_dir=work,
        target=_read_target(profiles_dir, profile or project_yml.get("profile"), target),
    )


def dbt_version_supported(version: str) -> bool:
    m = re.match(r"(\d+)\.(\d+)", version or "")
    if not m:
        return False
    v = (int(m.group(1)), int(m.group(2)))
    return SUPPORTED_DBT[0] <= v <= SUPPORTED_DBT[1]


def _dbt_argv(dbt_command: str | None) -> list[str]:
    if dbt_command:
        return dbt_command.split()
    # dbt installed next to this tool (`uvx --with dbt-bigquery dbt2dataform`)
    # comes with the adapter the user asked for; a dbt elsewhere on PATH may not.
    if importlib.util.find_spec("dbt.cli") is not None:
        # What the `dbt` console script runs; `-m dbt.cli.main` warns on stderr.
        return [sys.executable, "-c", "import sys; from dbt.cli.main import cli; sys.exit(cli())"]
    found = shutil.which("dbt")
    if found:
        return [found]
    raise ProjectError(
        "dbt is not installed. Install dbt-core and dbt-bigquery next to "
        "dbt2dataform (`uvx --with dbt-core --with dbt-bigquery dbt2dataform ...`), "
        "put dbt on PATH, or pass --dbt or --manifest."
    )


def _parse(
    project_dir: Path,
    profiles_dir: Path | None,
    target: str | None,
    dbt_command: str | None,
    profile: str | None,
) -> tuple[dict, dict[str, str], Path]:
    """Run `dbt parse` on a throwaway copy, so the user's tree is never touched.

    `dbt deps` writes dbt_packages/ and package-lock.yml, and parse writes
    target/ and logs/; none of that belongs in someone else's checkout. The
    copy stays until the conversion finishes, because package seeds and
    package YAML are read from its dbt_packages/.
    """
    work_root = Path(tempfile.mkdtemp(prefix="dbt2dataform-"))
    work = work_root / project_dir.name
    try:
        shutil.copytree(
            project_dir,
            work,
            ignore=shutil.ignore_patterns("target", "logs", ".venv", "venv", "__pycache__", ".git"),
            symlinks=True,
        )
        _absolutise_local_packages(work, project_dir)
        # dbt writes .user.yml into the profiles dir; when that dir is inside the
        # project, read it from the copy so the write lands there too.
        if profiles_dir == project_dir or project_dir in profiles_dir.parents:
            profiles_dir = work / profiles_dir.relative_to(project_dir)
        dbt = _dbt_argv(dbt_command)
        common = ["--project-dir", str(work), "--profiles-dir", str(profiles_dir)]
        if target:
            common += ["--target", target]
        if profile:
            common += ["--profile", profile]

        env = dict(os.environ)
        # Parsing needs no particular dbt version, so let a project that pins
        # one (`require-dbt-version`) through. The report records the version.
        env["DBT_VERSION_CHECK"] = "false"
        env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        supplied: dict[str, str] = {}
        needs_deps = any((work / f).exists() for f in _PACKAGE_FILES)
        if needs_deps and not (work / "dbt_packages").exists():
            deps = dbt + ["deps", "--project-dir", str(work), "--profiles-dir", str(profiles_dir)]
            _run(deps, env, supplied, "dbt deps")
        _run(
            dbt + ["parse", "--no-partial-parse", "--target-path", str(work / "target")] + common,
            env,
            supplied,
            "dbt parse",
        )
        manifest = json.loads((work / "target" / "manifest.json").read_text())
    except BaseException:
        shutil.rmtree(work_root, ignore_errors=True)
        raise
    return manifest, supplied, work_root


def _profiles_dir(project_dir: Path, profiles_dir: Path | None) -> Path:
    """Where dbt looks for profiles.yml, in dbt's order."""
    if profiles_dir is None:
        env_dir = os.environ.get("DBT_PROFILES_DIR")
        if env_dir:
            profiles_dir = Path(env_dir)
        elif (project_dir / "profiles.yml").exists():
            profiles_dir = project_dir
        else:
            profiles_dir = Path.home() / ".dbt"
    return profiles_dir.resolve()


# The parts of a profile output that dbt exposes as `target`, never credentials.
_TARGET_KEYS = ("schema", "dataset", "database", "project", "location", "threads", "type")


def _read_target(profiles_dir: Path, profile: str | None, target: str | None) -> dict:
    """dbt's `target` for the profile and target the project is parsed with.

    Values that are Jinja (env_var() and the like) are left out rather than
    guessed; the converter falls back to what the manifest shows.
    """
    path = profiles_dir / "profiles.yml"
    try:
        profiles = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return {"name": target} if target else {}
    entry = profiles.get(profile) if profile else None
    if not isinstance(entry, dict):
        return {"name": target} if target else {}
    name = target or entry.get("target")
    output = (entry.get("outputs") or {}).get(name) or {}
    values = {
        k: v
        for k, v in output.items()
        if k in _TARGET_KEYS and isinstance(v, (str, int)) and "{{" not in str(v)
    }
    # dbt-bigquery answers to both names.
    for a, b in (("schema", "dataset"), ("database", "project")):
        if a in values or b in values:
            values[a] = values[b] = values.get(a, values.get(b))
    return {"name": name, "profile_name": profile, **values} if name else values


def _absolutise_local_packages(work: Path, original: Path) -> None:
    """`local: ../pkg` is relative to the original project, not to the copy."""
    for name in _PACKAGE_FILES:
        path = work / name
        if not path.exists():
            continue
        doc = yaml.safe_load(path.read_text()) or {}
        changed = False
        for entry in doc.get("packages") or []:
            local = entry.get("local") if isinstance(entry, dict) else None
            if isinstance(local, str) and "{{" not in local and not Path(local).is_absolute():
                entry["local"] = str((original / local).resolve())
                changed = True
        if changed:
            path.write_text(yaml.safe_dump(doc, sort_keys=False))


def _run(argv: list[str], env: dict, supplied: dict, what: str) -> None:
    # dbt stops at the first env var it cannot resolve, so supply placeholders
    # one at a time until it gets through.
    for _ in range(25):
        proc = subprocess.run(argv, env=env, capture_output=True, text=True)
        if proc.returncode == 0:
            return
        output = proc.stdout + proc.stderr
        missing = _MISSING_ENV.search(output)
        if missing and missing.group(1) not in env:
            name = missing.group(1)
            env[name] = supplied[name] = ENV_PLACEHOLDER.format(name)
            continue
        raise ProjectError(f"{what} failed:\n{output[-4000:]}")
    raise ProjectError(f"{what} kept asking for more env vars: {sorted(supplied)}")
