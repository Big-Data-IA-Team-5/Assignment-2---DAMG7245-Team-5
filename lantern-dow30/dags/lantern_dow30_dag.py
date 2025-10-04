from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os, json
from jsonschema import validate

from lantern.tickers import get_companies
from lantern.discover_ir import discover_ir, make_record
from lantern.discover_reports import pick_latest
from lantern.downloader import download_file
from lantern.storage_gcs import make_key, upload_file, upload_json_bytes
from lantern.parser_docai import parse_pdf_with_docai
from lantern.schemas import DISCOVERY_SCHEMA, REPORT_CHOICE_SCHEMA, DOWNLOAD_MANIFEST_SCHEMA, PARSED_SCHEMA

# Airflow defaults
default_args = dict(owner="lantern", retries=1, retry_delay=timedelta(minutes=5))

with DAG(
    dag_id="lantern_dow30_docai_gcs",
    start_date=datetime(2025, 10, 1),
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    @task
    def list_companies():
        return get_companies()[:5]  # subset for demo

    @task
    def discover_ir_task(company):
        ticker, name, homepage = company
        ir = discover_ir(homepage, ticker)  # Pass ticker for company-specific patterns
        rec = make_record(ticker, name, homepage, ir)
        validate(rec, DISCOVERY_SCHEMA)
        return rec

    @task
    def discover_reports_task(discovery):
        from datetime import datetime
        ir_url = discovery["ir_url"]
        r = pick_latest(ir_url)
        # guess quarter by chosen first date (if present)
        q = None
        for ch in r["chosen"]:
            if ch.get("date"):
                dt = ch["date"]
                q = f"{dt.year}-Q{(dt.month - 1)//3 + 1}"
                break
        out = {"ticker": discovery["ticker"], "quarter": q, "candidates": r["candidates"], "chosen": r["chosen"]}
        validate(out, REPORT_CHOICE_SCHEMA)
        return out

    @task
    def download_task(report_choice):
        os.makedirs("/opt/airflow/data/raw", exist_ok=True)
        dls = []
        ticker = report_choice["ticker"]
        quarter = report_choice.get("quarter") or "unknown"
        for idx, ch in enumerate(report_choice["chosen"]):
            url = ch["href"]
            ext = url.split("?")[0].split("#")[0].split(".")[-1].lower() or "pdf"
            date_tag = ch["date"].date().isoformat() if ch.get("date") else "unknown"
            artifact_id = f"{ticker}_{quarter}_artifact{idx+1}_{date_tag}"
            out_path = f"/opt/airflow/data/raw/{artifact_id}.{ext}"
            man = download_file(url, out_path)
            if man:
                validate(man, DOWNLOAD_MANIFEST_SCHEMA)
                man["quarter"] = quarter
                dls.append(man)
        return dls

    @task
    def parse_and_upload_task(download_manifests):
        parsed_refs = []
        for man in download_manifests:
            ticker = man["artifact_id"].split("_")[0]
            quarter = man.get("quarter") or "unknown"
            ext = man["local_path"].split(".")[-1].lower()
            # Upload raw first
            raw_key = make_key(ticker, quarter, "raw", os.path.basename(man["local_path"]))
            upload_file(man["local_path"], raw_key)

            parsed = {
                "version": "1.0",
                "ticker": ticker,
                "quarter": quarter,
                "artifact_id": man["artifact_id"],
                "artifact_type": ext,
                "source_url": man["source_url"],
                "published_date": "",
                "extracted": {},
                "parse_status": "ok",
                "parse_notes": "",
                "metrics": {},
                "parsed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            if ext in ("pdf","ppt","pptx"):
                try:
                    doc = parse_pdf_with_docai(man["local_path"])
                    parsed["extracted"] = doc["extracted"]
                    parsed["metrics"] = doc["metrics"]
                except Exception as e:
                    parsed["parse_status"] = "failed"
                    parsed["parse_notes"] = str(e)
            else:
                parsed["parse_status"] = "partial"
                parsed["parse_notes"] = "Non-PDF not fully supported in DocAI stage."

            validate(parsed, PARSED_SCHEMA)
            # Upload parsed JSON
            fname = f"{man['artifact_id']}.json"
            parsed_key = make_key(ticker, quarter, "parsed", fname)
            upload_json_bytes(json.dumps(parsed, ensure_ascii=False, indent=2).encode("utf-8"), parsed_key)
            parsed_refs.append({"ticker": ticker, "quarter": quarter, "raw": raw_key, "parsed": parsed_key})
        return parsed_refs

    companies = list_companies()
    discoveries = discover_ir_task.expand(company=companies)
    reports = discover_reports_task.expand(discovery=discoveries)
    downloads = download_task.expand(report_choice=reports)
    parse_and_upload_task.expand(download_manifests=downloads)