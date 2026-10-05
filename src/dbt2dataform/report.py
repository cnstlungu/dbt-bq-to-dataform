"""Collects what happened during a conversion and writes CONVERSION_REPORT.md."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

# info:    converted, but worth knowing (a renamed column, a default chosen)
# warning: converted, behaviour differs from dbt in a way someone should review
# manual:  not converted, or converted into something that will not run as is
LEVELS = ("manual", "warning", "info")
_MAX_SUBJECTS = 10
_RESOURCE_TYPES = {"model", "seed", "snapshot", "test", "source", "analysis", "unit_test", "function"}


@dataclass
class Issue:
    level: str
    subject: str
    message: str


@dataclass
class Mapping:
    dbt_id: str
    dbt_path: str
    dataform_path: str
    dataform_type: str
    note: str = ""


@dataclass
class Report:
    project_name: str = ""
    dbt_version: str = ""
    issues: list[Issue] = field(default_factory=list)
    mappings: list[Mapping] = field(default_factory=list)
    skipped: Counter = field(default_factory=Counter)
    settings_vars: dict[str, tuple[str, str]] = field(default_factory=dict)

    def add(self, level: str, subject: str, message: str) -> None:
        assert level in LEVELS, level
        issue = Issue(level, subject, message)
        if issue not in self.issues:
            self.issues.append(issue)

    def manual(self, subject: str, message: str) -> None:
        self.add("manual", subject, message)

    def warning(self, subject: str, message: str) -> None:
        self.add("warning", subject, message)

    def info(self, subject: str, message: str) -> None:
        self.add("info", subject, message)

    def counts(self) -> Counter:
        return Counter(i.level for i in self.issues)

    def render(self) -> str:
        out: list[str] = []
        out.append(f"# Conversion report: {self.project_name}\n")
        out.append(
            f"Converted from a dbt-bigquery project (parsed with dbt-core "
            f"{self.dbt_version}) into a Dataform project by `dbt2dataform`.\n"
        )
        types = Counter(m.dataform_type for m in self.mappings)
        c = self.counts()
        out.append("## Summary\n")
        out.append("| | |\n|---|---|")
        for t, n in sorted(types.items()):
            out.append(f"| Dataform `{t}` actions written | {n} |")
        for what, n in sorted(self.skipped.items()):
            out.append(f"| dbt {what} not converted | {n} |")
        out.append(f"| Needs manual work | {c['manual']} |")
        out.append(f"| Warnings | {c['warning']} |")
        out.append(f"| Notes | {c['info']} |")
        out.append("")

        if self.settings_vars:
            out.append("## Variables to set in `workflow_settings.yaml`\n")
            out.append("| Variable | Came from | Value written |\n|---|---|---|")
            for name, (origin, value) in sorted(self.settings_vars.items()):
                out.append(f"| `{name}` | {origin} | `{value}` |")
            out.append("")

        for level, title in (
            ("manual", "Needs manual work"),
            ("warning", "Warnings"),
            ("info", "Notes"),
        ):
            grouped: dict[str, list[str]] = {}
            for i in self.issues:
                if i.level == level:
                    grouped.setdefault(i.message, []).append(self.short(i.subject))
            if not grouped:
                continue
            out.append(f"## {title}\n")
            for message, subjects in grouped.items():
                shown = ", ".join(subjects[:_MAX_SUBJECTS])
                if len(subjects) > _MAX_SUBJECTS:
                    shown += f" and {len(subjects) - _MAX_SUBJECTS} more"
                out.append(f"- **{shown}**: {message}")
            out.append("")

        out.append("## File mapping\n")
        out.append("| dbt | Dataform | Type | Note |\n|---|---|---|---|")
        for m in sorted(self.mappings, key=lambda m: m.dataform_path):
            out.append(
                f"| `{m.dbt_path}` | `{m.dataform_path}` | {m.dataform_type} | {m.note} |"
            )
        out.append("")
        return "\n".join(out)

    def short(self, subject: str) -> str:
        """`model.my_project.dim_date` -> `dim_date`; a package's -> `pkg.dim_date`."""
        parts = subject.split(".")
        if len(parts) >= 3 and parts[0] in _RESOURCE_TYPES:
            return ".".join(parts[2:]) if parts[1] == self.project_name else ".".join(parts[1:])
        return subject

    def write(self, path: Path) -> None:
        path.write_text(self.render())
