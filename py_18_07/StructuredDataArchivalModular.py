from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os, csv, requests, shutil
import xml.etree.ElementTree as ET

# Constants
WATCH_DIR = "/opt/airflow/data/incomingcsv/"
PROCESSED_DIR = "/opt/airflow/data/processedcsv/"

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="StructuredDataArchivalToIA",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Streaming CSV to XML export with disk persistence and retry-safe metrics",
    tags=["streaming", "resilient", "InfoArchive"],
    params={
        "CSV_FILENAME": "Schema_Table.csv",
        "SCHEMA_NAME": "your_schema",
        "CHUNK_SIZE": 5000,
        "BEARER_TOKEN": "your_token",
        "EXPORT_URL": "https://your.api.endpoint/export"
    }
) as dag:

    @task
    def find_csv_file():
        ctx = get_current_context()
        filename = ctx["params"]["CSV_FILENAME"]
        full_path = os.path.join(WATCH_DIR, filename)
        lock_path = full_path + ".lock"
        if os.path.exists(full_path) and not os.path.exists(lock_path):
            open(lock_path, "w").close()
            return {"file": full_path, "lock": lock_path}
        raise FileNotFoundError(f"📁 File '{filename}' not found or is locked")

    @task
    def extract_metadata(context):
        params = get_current_context()["params"]
        path = context["file"]
        table_name = os.path.splitext(os.path.basename(path))[0]
        return {
            "file": path,
            "lock": context["lock"],
            "schema": params["SCHEMA_NAME"],
            "table": table_name,
            "url": params["EXPORT_URL"],
            "chunk_size": params.get("CHUNK_SIZE", 5000),
            "bearer_token": params["BEARER_TOKEN"]
        }

    @task
    def read_and_store_chunks(metadata):
        chunk_size = metadata["chunk_size"]
        chunk_ids = []
        row_count = 0
        with open(metadata["file"], encoding="utf-8") as f:
            reader = csv.DictReader(f)
            chunk = []
            chunk_index = 0
            for row in reader:
                chunk.append(row)
                row_count += 1
                if len(chunk) >= chunk_size:
                    path = os.path.join(WATCH_DIR, f"{metadata['table']}_chunk_{chunk_index}.csv")
                    with open(path, "w", newline="", encoding="utf-8") as cf:
                        writer = csv.DictWriter(cf, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(chunk)
                    chunk_ids.append(chunk_index)
                    chunk = []
                    chunk_index += 1
            if chunk:
                path = os.path.join(WATCH_DIR, f"{metadata['table']}_chunk_{chunk_index}.csv")
                with open(path, "w", newline="", encoding="utf-8") as cf:
                    writer = csv.DictWriter(cf, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(chunk)
                chunk_ids.append(chunk_index)
        return {"metadata": metadata, "chunk_ids": chunk_ids, "total_rows": row_count}

    @task
    def export_chunks(context):
        metadata = context["metadata"]
        chunk_ids = context["chunk_ids"]
        exported_rows = 0
        for idx in chunk_ids:
            chunk_csv = os.path.join(WATCH_DIR, f"{metadata['table']}_chunk_{idx}.csv")
            chunk_xml = os.path.join(WATCH_DIR, f"{metadata['table']}_chunk_{idx}.xml")

            if not os.path.exists(chunk_xml):
                with open(chunk_csv, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    root = ET.Element(metadata["schema"])
                    middle = ET.SubElement(root, metadata["table"])
                    rows_in_chunk = 0
                    for row in reader:
                        row_elem = ET.SubElement(middle, "ROW")
                        for k, v in row.items():
                            child = ET.SubElement(row_elem, k.strip().replace(" ", "_"))
                            child.text = str(v) if v else ""
                        rows_in_chunk += 1
                    xml_data = ET.tostring(root, encoding="utf-8")
                with open(chunk_xml, "wb") as xf:
                    xf.write(xml_data)
            else:
                rows_in_chunk = sum(1 for _ in open(chunk_csv)) - 1

            with open(chunk_xml, "rb") as xf:
                response = requests.post(
                    url=metadata["url"],
                    headers={
                        "Authorization": f"Bearer {metadata['bearer_token']}",
                        "Content-Type": "application/xml"
                    },
                    data=xf.read()
                )

            print(f"📤 Exported chunk {idx} | Status: {response.status_code}")
            if response.status_code == 201:
                exported_rows += rows_in_chunk
        return {
            "metadata": metadata,
            "chunk_ids": chunk_ids,
            "exported_rows": exported_rows
        }

    @task
    def collect_oob_metrics(context):
        ctx = get_current_context()
        dr = ctx["dag_run"]
        sql = """
        INSERT INTO dag_run_metrics (
            dag_id, run_id, execution_date, records_processed
        )
        VALUES (
            %(dag_id)s, %(run_id)s, %(execution_date)s, %(records_processed)s
        )
        """
        parameters = {
            "dag_id": dr.dag_id,
            "run_id": dr.run_id,
            "execution_date": dr.logical_date,
            "records_processed": context["exported_rows"]
        }
        hook = PostgresHook(postgres_conn_id="metrics_db")
        hook.run(sql, parameters=parameters)
        print(f"📊 Metrics recorded for {context['exported_rows']} rows")

    @task
    def cleanup(context):
        metadata = context["metadata"]
        table = metadata["table"]
        lock = metadata["lock"]
        file = metadata["file"]
        for idx in context["chunk_ids"]:
            for ext in ["csv", "xml"]:
                path = os.path.join(WATCH_DIR, f"{table}_chunk_{idx}.{ext}")
                if os.path.exists(path):
                    os.remove(path)
        shutil.move(file, os.path.join(PROCESSED_DIR, os.path.basename(file)))
        os.remove(lock)
        print(f"🧹 Cleanup completed for {file}")

    # DAG Execution Flow
    file_ctx = find_csv_file()
    metadata_ctx = extract_metadata(file_ctx)
    chunks_ctx = read_and_store_chunks(metadata_ctx)
    export_ctx = export_chunks(chunks_ctx)
    collect_oob_metrics(export_ctx)
    cleanup(export_ctx)