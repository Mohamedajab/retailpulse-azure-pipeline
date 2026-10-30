import importlib.util
from pathlib import Path


def test_report_escapes_source_text(tmp_path, monkeypatch):
    path = Path(__file__).parents[1] / "scripts/build_report.py"
    spec = importlib.util.spec_from_file_location("report", path)
    report = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(report)
    monkeypatch.setattr(report, "ROOT", tmp_path)
    directory = tmp_path / 'data/curated'
    directory.mkdir(parents=True)
    for name in ['monthly_region_sales', 'product_popularity', 'pipeline_metrics']:
        (directory / (name + ".csv")).write_text("name,value\n<script>,2\n", encoding="utf-8")
    html = report.render()
    assert "&lt;script&gt;" in html
    assert "<script>" not in html
    assert html.count("<table>") == 3
