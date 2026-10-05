"""The optional settings file, dbt2dataform.yml, and the overrides it carries.

A settings file makes a conversion repeatable: a repository that holds a
generated Dataform project keeps the flags it was generated with next to it,
and regenerating is one command. Overrides add Dataform config the dbt
project cannot express, such as BigQuery partitioning for a project that
targets DuckDB.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath

import yaml

SETTINGS_FILE = "dbt2dataform.yml"
_KEYS = {
    "default_project",
    "default_location",
    "core_version",
    "vars",
    "contracts",
    "packages",
    "overrides",
}


class SettingsError(Exception):
    pass


def load_settings(path: Path) -> dict:
    try:
        doc = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise SettingsError(f"{path}: {exc}") from exc
    doc = doc or {}
    if not isinstance(doc, dict):
        raise SettingsError(f"{path}: expected a mapping at the top level")
    unknown = sorted(set(doc) - _KEYS)
    if unknown:
        raise SettingsError(f"{path}: unknown settings {unknown}; expected some of {sorted(_KEYS)}")
    variables = doc.get("vars") or {}
    if not isinstance(variables, dict):
        raise SettingsError(f"{path}: `vars` must be a mapping")
    # workflow_settings.yaml vars are strings; YAML would make 10 an int.
    doc["vars"] = {str(k): "" if v is None else str(v) for k, v in variables.items()}
    overrides = doc.get("overrides") or {}
    if not isinstance(overrides, dict) or not all(
        isinstance(k, str) and isinstance(v, dict) for k, v in overrides.items()
    ):
        raise SettingsError(f"{path}: `overrides` must map action selectors to config mappings")
    doc["overrides"] = overrides
    return doc


@dataclass
class Overrides:
    """Extra Dataform config, merged into the actions whose selector matches.

    A selector with a `/` is a glob over the action's file path under
    definitions/, without `.sqlx` (`staging/*`); any other selector is a glob
    over the action's name (`fact_sales`, `staging_*`, `*`). `*` matches across
    folders. Every matching rule applies, in file order.
    """

    rules: dict[str, dict] = field(default_factory=dict)
    used: set[str] = field(default_factory=set)

    def apply(self, cfg: dict, path: str) -> list[str]:
        rel = path.removeprefix("definitions/").removesuffix(".sqlx")
        name = cfg.get("name") or PurePosixPath(rel).name
        applied = []
        for selector, extra in self.rules.items():
            subject = rel if "/" in selector else name
            if not fnmatchcase(subject, selector):
                continue
            if cfg.get("type") == "declaration":
                # Dataform does not let declarations carry tags, so a broad
                # selector that tags every action has to leave them out.
                extra = {k: v for k, v in extra.items() if k != "tags"}
            merge_config(cfg, extra)
            self.used.add(selector)
            applied.append(selector)
        return applied

    def unused(self) -> list[str]:
        return [s for s in self.rules if s not in self.used]


def merge_config(base: dict, extra: dict) -> None:
    """Mappings merge, `tags` are added to, `null` removes, anything else replaces."""
    for key, value in extra.items():
        if value is None:
            base.pop(key, None)
        elif key == "tags":
            added = value if isinstance(value, list) else [value]
            base[key] = list(dict.fromkeys([*(base.get(key) or []), *added]))
        elif isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_config(base[key], value)
        else:
            base[key] = copy.deepcopy(value)
