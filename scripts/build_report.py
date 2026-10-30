"""Generate a standalone report from the pipeline's CSV exports."""
import csv
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def render():
    sections = []
    for name in ['monthly_region_sales', 'product_popularity', 'pipeline_metrics']:
        with (ROOT / 'data/curated' / (name + ".csv")).open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            headings = next(reader)
            rows = list(reader)
        header = "".join("<th>" + escape(value) + "</th>" for value in headings)
        body = "".join("<tr>" + "".join("<td>" + escape(value) + "</td>" for value in row) + "</tr>" for row in rows)
        sections.append("<section><h2>" + escape(name.replace("_", " ").title()) + "</h2><div><table><thead><tr>" + header + "</tr></thead><tbody>" + body + "</tbody></table></div></section>")
    return '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + 'RetailPulse' + '</title><style>body{font:16px system-ui;background:#f3f5fa;color:#17243a;margin:3%}h1{font-size:2.5rem}section{background:white;border-radius:12px;padding:24px;margin:24px 0}div{overflow:auto}table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:12px;border-bottom:1px solid #ddd}th{background:#eaf0fb}</style><h1>' + 'RetailPulse' + '</h1><p>Retail demonstration data. Generated from validated CSV exports.</p>' + "".join(sections) + '</html>'


if __name__ == "__main__":
    output = ROOT / "docs/report.html"
    output.write_text(render(), encoding="utf-8")
    print(output)
