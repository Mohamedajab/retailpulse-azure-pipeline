import csv
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

