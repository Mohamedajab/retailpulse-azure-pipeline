import csv
import shutil
from pathlib import Path

from retailpulse.pipeline import curate, deduplicate


def test_deduplicate_keeps_last_record():
    rows = [{"id": "1", "value": "old"}, {"id": "1", "value": "new"}]
    clean, removed = deduplicate(rows, "id")
    assert clean == [{"id": "1", "value": "new"}]
    assert removed == 1


def test_sample_pipeline_outputs_are_reconciled(tmp_path: Path):
    root = Path(__file__).parents[1]
    metrics = curate(root / "data/raw", tmp_path / "curated", tmp_path / "rejected")
    assert metrics["source_orders"] == metrics["accepted_orders"] + metrics["rejected_orders"] + 1
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

