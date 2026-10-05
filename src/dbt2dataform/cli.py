"""dbt2dataform command line."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .converter import DEFAULT_CORE_VERSION, DEFAULT_LOCATION, Converter, Options, write_output
from .dbt_project import ProjectError, load_project
from .settings import SETTINGS_FILE, SettingsError, load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dbt2dataform",
        description="Convert a dbt-bigquery project into a Dataform project.",
    )
    parser.add_argument("project_dir", type=Path, help="the dbt project (holds dbt_project.yml)")
    parser.add_argument("output_dir", type=Path, help="where to write the Dataform project")
    parser.add_argument(
        "--manifest",
        type=Path,
        help="an existing target/manifest.json; by default dbt parse runs on a temporary copy",
    )
    parser.add_argument("--profiles-dir", type=Path, help="passed to dbt parse")
    parser.add_argument("--profile", help="dbt profile to parse with (passed to dbt parse)")
    parser.add_argument("--target", help="dbt target to parse with")
    parser.add_argument("--dbt", dest="dbt_command", help="how to invoke dbt (default: dbt on PATH)")
    parser.add_argument(
        "--config",
        type=Path,
        help=f"settings file (default: {SETTINGS_FILE} in the output directory, if there is one)",
    )
    parser.add_argument("--default-project", help="GCP project for workflow_settings.yaml")
    parser.add_argument(
        "--default-location", help=f"BigQuery location (default: {DEFAULT_LOCATION})"
    )
    parser.add_argument(
        "--core-version", help=f"dataformCoreVersion to pin (default: {DEFAULT_CORE_VERSION})"
    )
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="set a var in workflow_settings.yaml, e.g. input_files_path=gs://bucket/parquet",
    )
    parser.add_argument(
        "--no-packages",
        action="store_true",
        help="convert only the project's own nodes, not those of installed packages",
    )
    parser.add_argument(
        "--no-contracts", action="store_true", help="skip assertions that port dbt model contracts"
    )
    parser.add_argument(
        "--force", action="store_true", help="write into a non-empty directory not made by dbt2dataform"
    )
    args = parser.parse_args(argv)

    config = args.config
    if config is None and (args.output_dir / SETTINGS_FILE).is_file():
        config = args.output_dir / SETTINGS_FILE
    settings: dict = {}
    if config is not None:
        try:
            settings = load_settings(config)
        except (OSError, SettingsError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print(f"Using settings from {config}")

    variables = dict(settings.get("vars") or {})
    for item in args.var:
        name, sep, value = item.partition("=")
        if not sep:
            parser.error(f"--var expects NAME=VALUE, got {item!r}")
        variables[name] = value

    try:
        project = load_project(
            args.project_dir,
            args.manifest,
            args.profiles_dir,
            args.target,
            args.dbt_command,
            args.profile,
        )
    except ProjectError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        converter = Converter(
            project,
            Options(
                default_project=args.default_project or settings.get("default_project"),
                default_location=args.default_location
                or settings.get("default_location")
                or DEFAULT_LOCATION,
                core_version=args.core_version or settings.get("core_version") or DEFAULT_CORE_VERSION,
                contracts=not args.no_contracts and settings.get("contracts", True),
                var_overrides=variables,
                overrides=settings.get("overrides") or {},
                include_packages=not args.no_packages and settings.get("packages", True),
                target_name=args.target,
            ),
        )
        files = converter.convert()
    finally:
        project.close()
    try:
        write_output(files, args.output_dir, args.force)
    except FileExistsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    counts = converter.report.counts()
    actions = sum(1 for p in files if p.startswith("definitions/"))
    disabled = len([u for u in converter.failed if u.startswith("model.")])
    print(f"Wrote {actions} Dataform actions to {args.output_dir}")
    if disabled:
        print(f"  {disabled} models could not be converted and were written disabled")
    print(
        f"  {counts['manual']} need manual work, {counts['warning']} warnings, "
        f"{counts['info']} notes: see {args.output_dir / 'CONVERSION_REPORT.md'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
