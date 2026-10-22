"""Checks that two runs produced the same output.

Documents are matched by key, not by line, and floats get a one-cent tolerance because the
engines add numbers up in a different order. Big outputs are compared on a fixed sample;
row counts and keys are always checked in full.
"""

import hashlib
import json
import math
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

from app.common.files import count_lines
from app.domain.models import Engine, OutputCheck, OutputName, VerificationResult

FLOAT_ABS_TOLERANCE = 0.011
FLOAT_REL_TOLERANCE = 1e-9
MAX_EXAMPLES = 5
DIGEST_MODULUS = 2**64
KEY_FIELDS: dict[OutputName, str] = {
    OutputName.TRADES: "trade_id",
    OutputName.REJECTS: "trade_id",
    OutputName.EXPOSURES: "ultimate_parent_id",
}


def first_difference(left: Any, right: Any, path: str = "$") -> str | None:
    """Return the JSON path of the first semantic difference, or None if equivalent."""
    left_is_number = isinstance(left, int | float) and not isinstance(left, bool)
    right_is_number = isinstance(right, int | float) and not isinstance(right, bool)
    if left_is_number and right_is_number:
        close = math.isclose(left, right, rel_tol=FLOAT_REL_TOLERANCE, abs_tol=FLOAT_ABS_TOLERANCE)
        return None if close else path
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return f"{path} keys {sorted(left.keys() ^ right.keys())}"
        for key in left:
            if (difference := first_difference(left[key], right[key], f"{path}.{key}")) is not None:
                return difference
        return None
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return f"{path} length {len(left)} != {len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
            if (difference := first_difference(left_item, right_item, f"{path}[{index}]")) is not None:
                return difference
        return None
    return None if left == right else path


@dataclass
class _Scan:
    rows: int = 0
    key_digest: int = 0
    sample: dict[str, Any] = field(default_factory=dict)


def _key_hash(key: str) -> int:
    return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest())


def _scan(path: Path, key_field: str, modulus: int) -> _Scan:
    """One pass: count rows, fold every key into an order-independent digest, keep a sample."""
    scan = _Scan()
    with path.open("rb") as handle:
        for line in handle:
            document = orjson.loads(line)
            key = str(document.get(key_field))
            scan.rows += 1
            scan.key_digest = (scan.key_digest + _key_hash(key)) % DIGEST_MODULUS
            if zlib.crc32(key.encode()) % modulus == 0:
                scan.sample[key] = document
    return scan


def compare_output(
    name: OutputName, left: Path, right: Path, full_max_rows: int, sample_rows: int
) -> tuple[OutputCheck, bool]:
    """Compare one NDJSON output; returns the check and whether it was sampled."""
    largest = max(count_lines(left), count_lines(right))
    modulus = 1 if largest <= full_max_rows else math.ceil(largest / sample_rows)
    left_scan = _scan(left, KEY_FIELDS[name], modulus)
    right_scan = _scan(right, KEY_FIELDS[name], modulus)

    examples: list[dict[str, Any]] = []
    mismatched = 0
    for key, left_doc in left_scan.sample.items():
        right_doc = right_scan.sample.get(key)
        difference = "missing on right" if right_doc is None else first_difference(left_doc, right_doc)
        if difference is None:
            continue
        mismatched += 1
        if len(examples) < MAX_EXAMPLES:
            examples.append({"key": key, "difference": difference})
    check = OutputCheck(
        output=name,
        left_rows=left_scan.rows,
        right_rows=right_scan.rows,
        keys_match=left_scan.key_digest == right_scan.key_digest,
        compared_docs=len(left_scan.sample),
        mismatched_docs=mismatched,
        examples=examples,
    )
    return check, modulus > 1


def verify_outputs(
    left_engine: Engine,
    left_outputs: dict[OutputName, str],
    right_engine: Engine,
    right_outputs: dict[OutputName, str],
    full_max_rows: int,
    sample_rows: int,
) -> VerificationResult:
    started = time.perf_counter()
    checks: list[OutputCheck] = []
    sampled = False
    for name in KEY_FIELDS:
        check, was_sampled = compare_output(
            name, Path(left_outputs[name]), Path(right_outputs[name]), full_max_rows, sample_rows
        )
        checks.append(check)
        sampled = sampled or was_sampled
    left_quality = json.loads(Path(left_outputs[OutputName.QUALITY_REPORT]).read_text())
    right_quality = json.loads(Path(right_outputs[OutputName.QUALITY_REPORT]).read_text())
    return VerificationResult(
        left_engine=left_engine,
        right_engine=right_engine,
        checks=checks,
        quality_reports_match=first_difference(left_quality, right_quality) is None,
        sampled=sampled,
        seconds=round(time.perf_counter() - started, 3),
    )
