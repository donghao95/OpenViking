# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import pytest

from openviking.parse.base import format_table_to_markdown
from openviking.parse.output import LocalParseOutputStore
from openviking.parse.parsers.markdown import MarkdownParser


@pytest.mark.parametrize("has_header", [True, False])
def test_table_omits_trailing_empty_rows(has_header):
    rows = [["Name", "Count"], ["alpha", "0"]]
    padded = rows + [["", ""], [" \t", "\n"], []] * 1000

    assert format_table_to_markdown(padded, has_header) == format_table_to_markdown(
        rows, has_header
    )
    assert len(padded) == 3002


@pytest.mark.parametrize("rows", [[], [[]], [["", ""], ["\t", "\n"]]])
def test_empty_table_has_no_markdown(rows):
    assert format_table_to_markdown(rows) == ""


@pytest.mark.parametrize("tail", [[], [["", ""]]])
def test_header_only_table_keeps_separator(tail):
    assert format_table_to_markdown([["Name", "Count"], *tail]) == (
        "| Name | Count |\n| --- | --- |"
    )


def test_table_preserves_leading_and_internal_empty_rows():
    rows = [["", ""], ["alpha", "0"], ["", ""], ["beta", "1"], ["", ""]]

    assert format_table_to_markdown(rows) == (
        "|  |  |\n| --- | --- |\n| alpha | 0 |\n|  |  |\n| beta | 1 |"
    )


def test_table_keeps_pipe_and_multiline_cells_before_empty_tail():
    assert format_table_to_markdown([["Value"], ["a|b\nc"], [" \t"]]) == (
        "| Value |\n| --- |\n| a\\|b<br>c |"
    )


@pytest.mark.asyncio
async def test_table_empty_tail_does_not_create_empty_chunks(tmp_path):
    rows = [["Name", "Count"], ["alpha", "0"], ["beta", "1"]] + [["", ""]] * 3000
    result = await MarkdownParser().parse_content(
        format_table_to_markdown(rows),
        source_path="sheet.md",
        parse_output_store=LocalParseOutputStore(local_root=str(tmp_path)),
    )

    files = list(tmp_path.rglob("*.md"))
    assert result.warnings == []
    assert len(files) == 1
    body = files[0].read_text(encoding="utf-8")
    assert "alpha" in body and "beta" in body


def test_empty_tail_preserves_original_column_count_and_falsey_values():
    rows = [["Value"], [0], [False], ["", "", ""]]
    assert format_table_to_markdown(rows) == (
        "| Value |  |  |\n| --- | --- | --- |\n| 0 |  |  |\n| False |  |  |"
    )


@pytest.mark.asyncio
async def test_long_cell_keeps_headers_and_does_not_inflate_internal_blank_rows(tmp_path):
    rows = [["ID", "Description"], ["1", "x" * 1200], ["", ""]]
    rows.extend([[str(index), "value-" + ("y" * 350)] for index in range(2, 12)])
    rows.extend([["", ""]] * 100)
    markdown = format_table_to_markdown(rows)
    assert "|  |  |" in markdown
    assert len(markdown) < 5000
    await MarkdownParser().parse_content(
        markdown,
        source_path="long-sheet.md",
        parse_output_store=LocalParseOutputStore(local_root=str(tmp_path)),
    )
    bodies = [file.read_text(encoding="utf-8") for file in tmp_path.rglob("*.md")]
    assert len(bodies) > 1
    assert all("| ID | Description |\n| --- | --- |" in body for body in bodies)
    assert sum(body.count("x") for body in bodies) == 1200
    assert sum(body.count("y") for body in bodies) == 3500
