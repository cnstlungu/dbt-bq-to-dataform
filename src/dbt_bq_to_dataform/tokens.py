"""Placeholders that carry Dataform constructs through Jinja rendering.

Rendering a dbt model emits a token such as `__df_tok_3__` wherever the
Dataform output needs JavaScript (`${ref("x")}`, `${self()}`, a project var).
A token is a plain identifier, so macros can pass it around, quote it or
concatenate it like any relation name, and it is swapped for its JavaScript
last.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

TOKEN_RE = re.compile(r"__df_tok_(\d+)__")


@dataclass(frozen=True)
class Replacement:
    # ref:   payload = (unique_id,)
    # self:  payload = ()
    # var:   payload = (dataform_var_name,)
    # js:    payload = (javascript_expression,)
    kind: str
    payload: tuple


class TokenRegistry:
    def __init__(self) -> None:
        self._tokens: dict[Replacement, str] = {}
        self._replacements: dict[str, Replacement] = {}

    def token(self, kind: str, *payload) -> str:
        rep = Replacement(kind, tuple(payload))
        tok = self._tokens.get(rep)
        if tok is None:
            tok = f"__df_tok_{len(self._tokens)}__"
            self._tokens[rep] = tok
            self._replacements[tok] = rep
        return tok

    def get(self, token: str) -> Replacement:
        return self._replacements[token]

    def tokens_in(self, text: str) -> list[str]:
        return [m.group(0) for m in TOKEN_RE.finditer(text)]
