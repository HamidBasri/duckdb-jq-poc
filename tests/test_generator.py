import pyarrow.parquet as pq
import pytest

from app.domain.contract import RAW_COLUMNS
from app.domain.models import Entity, GenerationSpec, SourceFormat
from app.infrastructure.generator import TradeDatasetGenerator, write_dataset


def test_same_spec_produces_identical_data() -> None:
    spec = GenerationSpec(rows=2_000, seed=11)
    first, second = TradeDatasetGenerator(spec).build(), TradeDatasetGenerator(spec).build()
    assert all(first[entity].equals(second[entity]) for entity in Entity)


def test_different_seed_changes_data() -> None:
    first = TradeDatasetGenerator(GenerationSpec(rows=2_000, seed=1)).build()[Entity.TRADES]
    second = TradeDatasetGenerator(GenerationSpec(rows=2_000, seed=2)).build()[Entity.TRADES]
    assert not first.equals(second)


@pytest.mark.parametrize("rows", [1_000, 5_000, 12_345])
def test_exact_row_count_and_contract_columns(rows: int) -> None:
    tables = TradeDatasetGenerator(GenerationSpec(rows=rows)).build()
    assert tables[Entity.TRADES].num_rows == rows
    for entity, table in tables.items():
        assert tuple(table.column_names) == RAW_COLUMNS[entity]


def test_deep_hierarchy_profile_reaches_requested_depth() -> None:
    spec = GenerationSpec(rows=1_000, hierarchy_depth=40, dirty_rate=0.0)
    counterparties = TradeDatasetGenerator(spec).build()[Entity.COUNTERPARTIES].to_pylist()
    parent_of = {row["counterparty_id"]: row["parent_counterparty_id"] for row in counterparties}

    def depth(node: str) -> int:
        steps = 0
        while (node := parent_of[node]) is not None:
            steps += 1
        return steps

    assert max(depth(node) for node in parent_of) == 39


def test_write_dataset_writes_every_format(tmp_path) -> None:  # type: ignore[no-untyped-def]
    written = write_dataset(GenerationSpec(rows=500), tmp_path)
    assert set(written.size_bytes) == set(SourceFormat)
    assert pq.read_table(tmp_path / "trades.parquet").num_rows == 500
