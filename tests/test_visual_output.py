"""Tests for the visual rendering of table and jsonp output."""

import re
import json
import pandas as pd

from cloudcat.formatters import format_table_with_colored_header, colorize_json

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return ANSI.sub("", text)


class TestTableRendering:
    def _df(self):
        return pd.DataFrame({
            "id": [1, 2, 42],
            "name": ["Alice", "Bob", "Dakota"],
            "price": [9.5, 1234.0, None],
            "active": [True, False, True],
        })

    def test_uses_rounded_frame(self):
        out = format_table_with_colored_header(self._df())
        assert "╭" in out and "╰" in out  # rounded corners

    def test_null_renders_as_dim_marker_not_nan(self):
        out = format_table_with_colored_header(self._df())
        plain = _plain(out)
        assert "∘" in plain
        assert "nan" not in plain  # the old broken rendering is gone

    def test_booleans_are_lowercase(self):
        out = format_table_with_colored_header(self._df())
        plain = _plain(out)
        assert "true" in plain and "false" in plain
        assert "True" not in plain and "False" not in plain

    def test_numbers_and_bools_are_colored(self):
        out = format_table_with_colored_header(self._df())
        # green for true, red for false, cyan for numbers, dim for null
        assert "\x1b[32mtrue" in out
        assert "\x1b[31mfalse" in out
        assert "\x1b[36m" in out  # cyan numbers/headers

    def test_alignment_preserved_despite_color_codes(self):
        out = format_table_with_colored_header(self._df())
        widths = {len(_plain(line)) for line in out.splitlines() if line.strip()}
        # Every rendered row (borders + data) has the same visible width.
        assert len(widths) == 1

    def test_empty_dataframe(self):
        assert format_table_with_colored_header(pd.DataFrame()) == "Empty dataset"

    def test_data_values_present(self):
        out = _plain(format_table_with_colored_header(self._df()))
        for token in ["Alice", "Bob", "Dakota", "9.5", "1234.0", "42"]:
            assert token in out


class TestJsonpRendering:
    def test_nested_structures_are_indented(self):
        data = [{"id": 1, "tags": ["a", "b"], "meta": {"k": 1}}]
        out = colorize_json(json.dumps(data))
        plain = _plain(out)
        # Nested array/object members appear on their own indented lines.
        assert "\n      \"k\"" in plain or "\n      " in plain
        # Deep nesting indents further than top-level keys.
        assert plain.count("\n") >= 6

    def test_empty_containers_stay_compact(self):
        out = _plain(colorize_json('[{"tags": [], "meta": {}}]'))
        assert "[]" in out and "{}" in out

    def test_scalar_colors(self):
        out = colorize_json('[{"n": 1, "s": "x", "b": true, "z": null}]')
        assert "\x1b[36m1" in out      # number cyan
        assert "\x1b[32m\"x\"" in out  # string green
        assert "\x1b[32mtrue" in out   # bool true green
        assert "\x1b[2mnull" in out    # null dim

    def test_keys_are_colored(self):
        out = colorize_json('[{"name": "Alice"}]')
        assert "\x1b[34m\"name\"" in out  # key blue
        assert "Alice" in _plain(out)

    def test_invalid_json_returned_unchanged(self):
        assert colorize_json("not json {") == "not json {"

    def test_single_object_renders(self):
        out = _plain(colorize_json('{"a": 1, "b": 2}'))
        assert out.startswith("{")
        assert "\"a\"" in out and "\"b\"" in out
