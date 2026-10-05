"""Writing SQLX: config blocks, incremental branches, token substitution."""

from __future__ import annotations

import difflib
import json
import re
from collections.abc import Callable

from .tokens import TOKEN_RE, Replacement

_JS_IDENT = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")


class JS(str):
    """A config value written as raw JavaScript rather than a string literal."""


def js_value(value, indent: int = 2, level: int = 1) -> str:
    pad = " " * (indent * level)
    inner = " " * (indent * (level + 1))
    if isinstance(value, JS):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        items = [js_value(v, indent, level + 1) for v in value]
        one_line = "[" + ", ".join(items) + "]"
        if len(one_line) <= 80 and "\n" not in one_line:
            return one_line
        return "[\n" + ",\n".join(inner + i for i in items) + "\n" + pad + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        parts = [f"{inner}{js_key(k)}: {js_value(v, indent, level + 1)}" for k, v in value.items()]
        return "{\n" + ",\n".join(parts) + "\n" + pad + "}"
    raise TypeError(f"cannot write {type(value).__name__} into a config block")


def js_key(key: str) -> str:
    return key if _JS_IDENT.match(key) else json.dumps(key)


def js_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def config_block(cfg: dict) -> str:
    cfg = {k: v for k, v in cfg.items() if v not in (None, [], {}, "")}
    lines = [f"  {js_key(k)}: {js_value(v)}" for k, v in cfg.items()]
    return "config {\n" + ",\n".join(lines) + "\n}\n"


def operations_block(kind: str, statements: list[str]) -> str:
    body = ";\n".join(s.strip().rstrip(";") for s in statements if s.strip())
    indented = "\n".join(f"  {line}" if line.strip() else "" for line in body.split("\n"))
    return f"{kind} {{\n{indented}\n}}\n"


def sqlx_file(cfg: dict, body: str, pre: list[str] | None = None, post: list[str] | None = None) -> str:
    parts = [config_block(cfg)]
    if pre:
        parts.append(operations_block("pre_operations", pre))
    if post:
        parts.append(operations_block("post_operations", post))
    if body.strip():
        parts.append(body.strip("\n") + "\n")
    return "\n".join(parts)


def _template_literal(sql: str) -> str:
    # Inside ${...} we are writing JavaScript, so a nested branch is a
    # template literal of its own and has to be escaped like one.
    escaped = sql.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    return f"`{escaped}`"


def merge_incremental(full: str, incremental: str) -> str:
    """One SQLX body that reproduces both renders of a dbt incremental model.

    dbt branches with {% if is_incremental() %}; Dataform with
    ${when(incremental(), ...)}. Rather than translate Jinja control flow, the
    model is rendered both ways and translated, and only the lines that
    differ are wrapped, so each branch compiles to exactly what dbt would run.
    """
    if full == incremental:
        return full
    a, b = full.split("\n"), incremental.split("\n")
    matcher = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    out: list[str] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            out.extend(a[i1:i2])
            continue
        inc = "\n".join(b[j1:j2])
        fr = "\n".join(a[i1:i2])
        if fr:
            out.append(f"${{when(incremental(), {_template_literal(inc)}, {_template_literal(fr)})}}")
        else:
            out.append(f"${{when(incremental(), {_template_literal(inc)})}}")
    return "\n".join(out)


def tidy_sql(sql: str) -> str:
    """Rendered Jinja without its whitespace debris: trailing spaces, runs of blank lines."""
    lines = [line.rstrip() for line in sql.replace("\r\n", "\n").split("\n")]
    out: list[str] = []
    for line in lines:
        if not line and (not out or not out[-1]):
            continue
        out.append(line)
    while out and not out[-1]:
        out.pop()
    return "\n".join(out) + "\n" if out else ""


def replace_tokens(text: str, lookup: Callable[[str], Replacement], to_js: Callable[[Replacement], str]) -> str:
    return TOKEN_RE.sub(lambda m: "${" + to_js(lookup(m.group(0))) + "}", text)


def commented(text: str) -> str:
    return "\n".join(f"-- {line}" if line.strip() else "--" for line in text.strip("\n").split("\n"))
