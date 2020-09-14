import os
from segments import SEGMENT_PATTERN
from typing import List

import pytest

from binio import kv_iter, kv_writer
from compaction import File, merge

FIXTURES = "tests/fixtures/files_precompaction"


@pytest.fixture
def files_pre_compaction() -> List[File]:
    compacted_test_file = f"{FIXTURES}/segment-0-compacted.dat"
    if os.path.exists(compacted_test_file):
        os.remove(compacted_test_file)
    files = []
    for file_ in os.scandir(f"{FIXTURES}"):
        if match := SEGMENT_PATTERN.search(file_.name):
            files.append(File(file_.path, -1, int(match.group("index"))))
    return files


def test_merge_correctly_merges_files(files_pre_compaction):
    expected = list(kv_iter(f"{FIXTURES}/segment-0-compacted-expected.dat"))
    merged_file = merge(files_pre_compaction)
    result = list(kv_iter(merged_file.path))
    assert result == expected


def generate_fixtures() -> None:
    segment_0 = [
        ("handbag", "0"),
        ("handful", "0"),
        ("handicap", "0"),
        ("handkerchief", "0"),
        ("handlebars", "0"),
        ("handprinted", "0"),
    ]
    segment_1 = [
        ("handcuffs", "1"),
        ("handful", "1"),
        ("handicap", "1"),
        ("handiwork", "1"),
        ("handkerchief", "1"),
        ("handprinted", "1"),
    ]
    segment_2 = [
        ("handful", "2"),
        ("handicap", "2"),
        ("handiwork", "2"),
        ("handlebars", "2"),
        ("handoff", "2"),
        ("handprinted", "2"),
    ]
    for name, pairs in locals().items():
        with kv_writer(f"{FIXTURES}/{name.replace('_', '-')}.dat") as writer:
            for pair in pairs:
                writer.write_entry(*pair)

    expected = [
        ("handbag", "0"),
        ("handcuffs", "1"),
        ("handful", "2"),
        ("handicap", "2"),
        ("handiwork", "2"),
        ("handkerchief", "1"),
        ("handlebars", "2"),
        ("handoff", "2"),
        ("handprinted", "2"),
    ]
    with kv_writer(f"{FIXTURES}/segment-0-compacted-expected.dat") as writer:
        for pair in expected:
            writer.write_entry(*pair)