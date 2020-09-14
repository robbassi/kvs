import os
from segments import SEGMENT_PATTERN
from typing import List

import pytest

from binio import kv_iter
from compaction import File, merge

FIXTURES = "tests/fixtures"


@pytest.fixture
def files_pre_compaction() -> List[File]:
    compacted_test_file = f"{FIXTURES}/files_precompaction/segment-0-compacted.dat"
    if os.path.exists(compacted_test_file):
        os.remove(compacted_test_file)
    files = []
    for file_ in os.scandir(f"{FIXTURES}/files_precompaction"):
        if match := SEGMENT_PATTERN.search(file_.name):
            files.append(File(file_.path, -1, int(match.group("index"))))
    return files


def test_merge_correctly_merges_files(files_pre_compaction):
    expected = list(
        kv_iter(f"{FIXTURES}/files_precompaction/segment-0-compacted-expected.dat")
    )
    merged_file = merge(files_pre_compaction)
    result = list(kv_iter(merged_file.path))
    assert result == expected
