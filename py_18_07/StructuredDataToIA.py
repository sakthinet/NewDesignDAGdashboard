from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
import os, csv, requests, shutil
import xml.etree.ElementTree as ET
from airflow.providers.postgres.hooks.postgres import PostgresHook

WATCH_DIR = "/opt/airflow/data/incomingcsv/"
PROCESSED_DIR = "/opt/airflow/data/processedcsv/"

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="StructuredDataToIA",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="CSV to XML transformation with param-driven schema/table names",
    tags=["file-specific", "InfoArchive", "API-driven"],
    params={
        "CSV_FILENAME": "Schema_Table.csv",
        "SCHEMA_NAME": "your_schema",
        "CHUNK_SIZE": 5000,
        "BEARER_TOKEN": "your_token",
        "EXPORT_URL": "Export URL for Table"
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
        raise FileNotFoundError(f"📁 Requested file '{filename}' not found or locked")

    @task
    def extract_metadata(context):
        ctx = get_current_context()
        params = ctx["params"]

        path = context["file"]
        table_name = os.path.splitext(os.path.basename(path))[0]
        schema = params.get("SCHEMA_NAME")

        return {
            "file": path,
            "lock": context["lock"],
            "base": table_name,
            "schema": schema,
            "table": table_name,
            "url": params.get("EXPORT_URL"),
            "chunk_size": params.get("CHUNK_SIZE", 5000),
            "bearer_token": params.get("BEARER_TOKEN")
        }

    @task
    def read_csv_chunks(metadata):
        chunks = []
        with open(metadata["file"], encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= metadata["chunk_size"]:
                    chunks.append(chunk)
                    chunk = []
            if chunk:
                chunks.append(chunk)
        return {"metadata": metadata, "chunks": chunks}

    @task
    def export_chunks(context):
        metadata = context["metadata"]
        for idx, chunk in enumerate(context["chunks"]):
            root = ET.Element(metadata["schema"])
            middle = ET.SubElement(root, metadata["table"])
            for row in chunk:
                row_elem = ET.SubElement(middle, "ROW")
                for k, v in row.items():
                    child = ET.SubElement(row_elem, k.strip().replace(" ", "_"))
                    child.text = str(v) if v else ""
            xml_data = ET.tostring(root, encoding="utf-8")

            temp_file = os.path.join(WATCH_DIR, f"{metadata['table']}_chunk_{idx}.xml")
            with open(temp_file, "wb") as f:
                f.write(xml_data)

            with open(temp_file, "rb") as f:
                xml_content = f.read()
                response = requests.post(
                    url=metadata["url"],
                    headers={
                        "Authorization": f"Bearer {metadata['bearer_token']}",
                        "Content-Type": "application/xml"
                    },
                    data=xml_content
                )
            print(f"📤 Exported chunk {idx} | Status: {response.status_code}")
            if response.status_code == 201:
                 rocords_migrated =rocords_migrated 
            os.remove(temp_file)

    @task()
    def collect_oob_metrics(ti=None):
        ctx = get_current_context()
        dr = ctx["dag_run"]
        dag_id = dr.dag_id
        run_id = dr.run_id
        exec_dt = dr.logical_date

        chunks_context = ti.xcom_pull(task_ids="export_chunks")
        chunks = chunks_context.get("chunks", [])
        total_rows = sum(len(chunk) for chunk in chunks)

        sql = """
        INSERT INTO dag_run_metrics (
            dag_id, run_id, execution_date, records_processed
        )
        VALUES (
            %(dag_id)s, %(run_id)s, %(execution_date)s, %(records_processed)s
        )
        """
        parameters = {
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": exec_dt,
            "records_processed": total_rows,
        }
        hook = PostgresHook(postgres_conn_id="metrics_db")
        hook.run(sql, parameters=parameters)

    @task
    def cleanup(context):
        shutil.move(context["metadata"]["file"], os.path.join(PROCESSED_DIR, os.path.basename(context["metadata"]["file"])))
        os.remove(context["metadata"]["lock"])
        print(f"✅ Cleanup done: {context['metadata']['file']}")

    

    # Execution flow
    file_context = find_csv_file()
    metadata_context = extract_metadata(file_context)
    chunks_context = read_csv_chunks(metadata_context)
    report_metrics = collect_oob_metrics()
    export_chunks(chunks_context) >> cleanup(chunks_context) >> report_metrics
