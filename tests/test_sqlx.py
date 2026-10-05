from dbt2dataform.sqlx import JS, config_block, merge_incremental
from dbt2dataform.tokens import TokenRegistry
from dbt2dataform.sqlx import replace_tokens


def test_identical_renders_are_left_alone():
    assert merge_incremental("SELECT 1", "SELECT 1") == "SELECT 1"


def test_only_differing_lines_are_branched():
    full = "SELECT a\nFROM t"
    inc = "SELECT a\nFROM t\nWHERE a > (SELECT MAX(a) FROM __df_tok_0__)"
    assert merge_incremental(full, inc) == (
        "SELECT a\nFROM t\n${when(incremental(), `WHERE a > (SELECT MAX(a) FROM __df_tok_0__)`)}"
    )


def test_replaced_lines_get_both_branches():
    merged = merge_incremental("WITH b AS (", "WITH a AS (x), b AS (")
    assert merged == "${when(incremental(), `WITH a AS (x), b AS (`, `WITH b AS (`)}"


def test_branch_text_is_escaped_for_a_template_literal():
    merged = merge_incremental("SELECT 1", "SELECT `col`, '\\d', '${x}'")
    assert merged == "${when(incremental(), `SELECT \\`col\\`, '\\\\d', '\\${x}'`, `SELECT 1`)}"


def test_tokens_inside_branches_become_interpolations():
    reg = TokenRegistry()
    tok = reg.token("self")
    text = merge_incremental("SELECT 1", f"SELECT MAX(a) FROM {tok}")
    out = replace_tokens(text, reg.get, lambda rep: "self()")
    assert out == "${when(incremental(), `SELECT MAX(a) FROM ${self()}`, `SELECT 1`)}"


def test_config_block_formatting():
    block = config_block(
        {
            "type": "table",
            "schema": JS('dataform.projectConfig.defaultSchema + "_raw"'),
            "tags": [],
            "description": 'say "hi"',
            "columns": {"a b": "spaced", "c": "plain"},
        }
    )
    assert block == (
        "config {\n"
        '  type: "table",\n'
        '  schema: dataform.projectConfig.defaultSchema + "_raw",\n'
        '  description: "say \\"hi\\"",\n'
        "  columns: {\n"
        '    "a b": "spaced",\n'
        '    c: "plain"\n'
        "  }\n"
        "}\n"
    )
