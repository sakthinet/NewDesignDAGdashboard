from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import os, csv, xml.etree.ElementTree as ET, requests, shutil

WATCH_DIR = "/opt/airflow/data/incomingcsv/"
CHUNK_TEMP_DIR = "/opt/airflow/data/incomingcsv/"
PROCESSED_DIR = "/opt/airflow/data/processedcsv/"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="StructuredDataToIA_Resilient",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Export CSV in parallel XML chunks with metrics tracking",
    tags=["chunked", "resilient", "InfoArchive"],
    params={
        "CSV_FILENAME": "PurchaseOrderHeader.csv",
        "SCHEMA_NAME": "SAP-PO1",
        "CHUNK_SIZE": 5000,
        "BEARER_TOKEN": "your_token",
        "EXPORT_URL": "https://api.example.com/export"
    }
) as dag:

    @task
    def get_chunk_meta():
        params = get_current_context()["params"]
        path = os.path.join(WATCH_DIR, params["CSV_FILENAME"])
        chunk_size = params["CHUNK_SIZE"]

        with open(path, newline="", encoding="utf-8") as f:
            total_rows = sum(1 for _ in f) - 1
        total_chunks = (total_rows + chunk_size - 1) // chunk_size

        return {
            "file": path,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "schema": params["SCHEMA_NAME"],
            "table": os.path.splitext(os.path.basename(path))[0],
            "bearer_token": params["BEARER_TOKEN"],
            "export_url": params["EXPORT_URL"]
        }

    @task
    def get_chunk_indexes(chunk_meta):
        return [(i, chunk_meta) for i in range(chunk_meta["total_chunks"])]

    @task
    def extract_chunk(args):
        chunk_index, chunk_meta = args
        start = chunk_index * chunk_meta["chunk_size"]
        end = start + chunk_meta["chunk_size"]
        rows = []

        with open(chunk_meta["file"], newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if start <= i < end:
                    rows.append(row)

        temp_csv = os.path.join(CHUNK_TEMP_DIR, f"chunk_{chunk_index}.csv")
        with open(temp_csv, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return (chunk_index, temp_csv, chunk_meta)

    @task
    def create_xml(args):
        chunk_index, chunk_csv_path, chunk_meta = args
        root = ET.Element(chunk_meta["schema"])
        middle = ET.SubElement(root, chunk_meta["table"])

        with open(chunk_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_elem = ET.SubElement(middle, "ROW")
                for k, v in row.items():
                    child = ET.SubElement(row_elem, k.strip().replace(" ", "_"))
                    child.text = str(v) if v else ""

        xml_path = os.path.join(CHUNK_TEMP_DIR, f"{chunk_meta['table']}_chunk_{chunk_index}.xml")
        with open(xml_path, "wb") as xf:
            xf.write(ET.tostring(root, encoding="utf-8"))

        return (chunk_index, xml_path, chunk_meta)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def export_chunk(args):
        chunk_index, xml_path, chunk_meta = args
        with open(xml_path, "rb") as f:
            response = requests.post(
                url=chunk_meta["export_url"],
                headers={
                    "Authorization": f"Bearer {chunk_meta["bearer_token"]}",
                    "Content-Type": "application/xml"
                },
                data=f.read()
            )

        if response.status_code != 201:
            raise Exception(f"❌ Chunk {chunk_index} export failed (status {response.status_code})")
        print(f"✅ Chunk {chunk_index} exported successfully")

    @task
    def collect_metrics(chunk_meta):
        ctx = get_current_context()
        rows = chunk_meta["chunk_size"] * chunk_meta["total_chunks"]

        sql = """
        INSERT INTO dag_run_metrics (
            dag_id, run_id, execution_date, records_processed
        )
        VALUES (
            %(dag_id)s, %(run_id)s, %(execution_date)s, %(records_processed)s
        )
        """

        params = {
            "dag_id": ctx["dag"].dag_id,
            "run_id": ctx["run_id"],
            "execution_date": ctx["logical_date"],
            "records_processed": rows
        }

        hook = PostgresHook(postgres_conn_id="metrics_db")
        hook.run(sql, parameters=params)
        print(f"📈 Logged: {rows} rows")

    @task
    def finalize(chunk_meta):
        shutil.move(
        chunk_meta["file"],
        os.path.join(PROCESSED_DIR, os.path.basename(chunk_meta["file"]))
    )
        print(f"📦 Archived original file: {chunk_meta['file']}")

    # Remove all chunked XML and CSV files
        for filename in os.listdir(CHUNK_TEMP_DIR):
            if (
                filename.endswith(".xml") and filename.startswith(chunk_meta["table"])
            ) or (
                filename.endswith(".csv") and filename.startswith("chunk_")
            ):
                try:
                    file_path = os.path.join(CHUNK_TEMP_DIR, filename)
                    os.remove(file_path)
                    print(f"🧹 Removed temp file: {file_path}")
                except Exception as e:
                    print(f"⚠️ Could not remove {file_path}: {e}")
        

    # 🔗 DAG Flow
    chunk_meta = get_chunk_meta()
    chunk_args = get_chunk_indexes(chunk_meta)

    extracted = extract_chunk.expand(args=chunk_args)
    xmls = create_xml.expand(args=extracted)
    export_chunk.expand(args=xmls) >> collect_metrics(chunk_meta) >> finalize(chunk_meta)