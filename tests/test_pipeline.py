import csv
import shutil
from itertools import permutations
from pathlib import Path

import pytest

from retailpulse.pipeline import curate, deduplicate


def test_deduplicate_newest_normalised_timestamp_then_source_values():
    rows = [
        {"id": "1", "value": "old", "updated_at": "2025-01-01T11:00:00Z"},
        {"id": "1", "value": "a", "updated_at": "2025-01-01T14:00:00+02:00"},
        {"id": "1", "value": "z", "updated_at": "2025-01-01T12:00:00"},
    ]
    for reordered in permutations(rows):
        clean, removed = deduplicate(list(reordered), "id")
        assert clean == [{"id": "1", "value": "z", "updated_at": "2025-01-01T12:00:00.000000Z"}]
        assert removed == 2


@pytest.mark.parametrize("timestamp", [None, "", "wrong", "2025-02-30T10:00:00", "2025-01-01", "2025-01-01T00:00:00+00:99"])
def test_invalid_timestamp_fails_even_on_superseded_row(timestamp):
    with pytest.raises(ValueError):
        deduplicate([{"id": "1", "updated_at": timestamp}, {"id": "1", "updated_at": "2026-01-01T00:00:00Z"}], "id")


def test_sample_pipeline_outputs_are_reconciled(tmp_path: Path):
    root = Path(__file__).parents[1]
    metrics = curate(root / "data/raw", tmp_path / "curated", tmp_path / "rejected")
    assert metrics["source_orders"] == metrics["accepted_orders"] + metrics["rejected_orders"] + metrics["order_duplicates_removed"]
    assert metrics["duplicate_records_removed"] == 3
    with (tmp_path / "curated/sales_detail.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert all(float(row["revenue"]) >= 0 for row in rows)
    assert {row["region"] for row in rows} >= {"North", "South"}


def test_empty_orders_still_produces_outputs(tmp_path: Path):
    raw = tmp_path / "raw"
    shutil.copytree(Path(__file__).parents[1] / "data/raw", raw)
    (raw / "orders.csv").write_text(
        "order_id,order_date,customer_id,product_id,quantity,unit_price,updated_at\n",
        encoding="utf-8",
    )
    metrics = curate(raw, tmp_path / "curated", tmp_path / "rejected")
    assert metrics["source_orders"] == 0
    with (tmp_path / "rejected/orders.csv").open() as handle:
        assert list(csv.DictReader(handle)) == []


def test_unrepresentable_price_is_quarantined(tmp_path: Path):
    raw = tmp_path / "raw"
    shutil.copytree(Path(__file__).parents[1] / "data/raw", raw)
    with (raw / "orders.csv").open("a", encoding="utf-8") as handle:
        handle.write("O9999,2025-01-01,C001,P100,1,1e999,2025-01-01T00:00:00\n")
    metrics = curate(raw, tmp_path / "curated", tmp_path / "rejected")
    assert metrics["source_orders"] == 9
    assert metrics["rejected_orders"] == 3

