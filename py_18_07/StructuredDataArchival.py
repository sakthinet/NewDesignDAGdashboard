from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os, csv, requests, shutil
import xml.etree.ElementTree as ET

# Configs
WATCH_DIR = "/opt/airflow/data/incomingcsv/"
PROCESSED_DIR = "/opt/airflow/data/processedcsv/"
CHUNK_SIZE = 5000
BEARER_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdWVAaWFjdXN0b21lci5jb20iLCJ1c2VyX25hbWUiOiJzdWVAaWFjdXN0b21lci5jb20iLCJpc3MiOiJodHRwOi8vMTAuNzMuOTEuMjM6ODA4MCIsInRva2VuX3R5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiJpbmZvYXJjaGl2ZS5jbGkiLCJhdXRob3JpdGllcyI6WyJHUk9VUF9SRVRFTlRJT05fTUFOQUdFUiIsIkdST1VQX0RFVkVMT1BFUiIsIkdST1VQX0lUX09XTkVSIiwiR1JPVVBfQlVTSU5FU1NfT1dORVIiLCJHUk9VUF9BRE1JTklTVFJBVE9SIiwiR1JPVVBfRU5EX1VTRVIiLCJST0xFX0FETUlOSVNUUkFUT1IiLCJST0xFX0JVU0lORVNTX09XTkVSIiwiUk9MRV9ERVZFTE9QRVIiLCJST0xFX0VORF9VU0VSIiwiUk9MRV9JVF9PV05FUiIsIlJPTEVfUkVURU5USU9OX01BTkFHRVIiXSwiYXVkIjoiaW5mb2FyY2hpdmUuY2xpIiwibmJmIjoxNzUxODcwMTM4LCJncmFudF90eXBlIjoicGFzc3dvcmQiLCJzY29wZSI6WyJzZWFyY2giXSwiZXhwIjozODk5MzUzNzg1LCJpYXQiOjE3NTE4NzAxMzgsImp0aSI6IjQzZjE3MzM3LTdiZWItNGJlOC04NWMzLTZhYzFjMWEzZTA4ZCJ9.OEOX1DoN_d-YHKDBDf290bYRrr2IiujjQqkolLSh34M"
EXPORT_URL_MAP = {
    "PurchaseOrderHeader": "http://10.73.91.23:8765/systemdata/tables/AAAAhw/content",
    "PurchaseOrderItem": "http://10.73.91.23:8765/systemdata/tables/AAAAhg/content",
    "PurchaseOrderPartner": "http://10.73.91.23:8765/systemdata/tables/AAAAhQ/content"
}

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="StructuredDataArchival",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    description="Schema and Table Detection,CSV to XML Transformation ,Export with IA",
    tags=['Schema & Table Detection','CSV to XML','Export to InfoArchive']
) as dag:

    @task
    def find_csv_file():
        for fname in os.listdir(WATCH_DIR):
            if fname.endswith(".csv"):
                full_path = os.path.join(WATCH_DIR, fname)
                lock_path = full_path + ".lock"
                if not os.path.exists(lock_path):
                    open(lock_path, 'w').close()  # create lock
                    return {"file": full_path, "lock": lock_path}
        raise FileNotFoundError("📁 No unprocessed CSV file found")

    @task
    def extract_metadata(context):
        path = context["file"]
        base = os.path.splitext(os.path.basename(path))[0]
        parts = base.split("_")
        schema = parts[0] if parts else "Schema"
        table = parts[1] if len(parts) > 1 else "Unknown"
        url = EXPORT_URL_MAP.get(table)
        if not url:
            raise ValueError(f"🚫 Unsupported table '{table}'")
        return {
            "file": path,
            "lock": context["lock"],
            "base": base,
            "schema": schema,
            "table": table,
            "url": url
        }

    @task
    def read_csv_chunks(metadata):
        chunks = []
        with open(metadata["file"], encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= CHUNK_SIZE:
                    chunks.append(chunk)
                    chunk = []
            if chunk:
                chunks.append(chunk)
        return {"metadata": metadata, "chunks": chunks}

    @task
    def export_chunks(context):
        metadata = context["metadata"]
        for idx, chunk in enumerate(context["chunks"]):
            # Convert chunk to XML
            root = ET.Element(metadata["schema"])
            middle = ET.SubElement(root, metadata["table"])
            for row in chunk:
                row_elem = ET.SubElement(middle, "ROW")
                for k, v in row.items():
                    child = ET.SubElement(row_elem, k.strip().replace(" ", "_"))
                    child.text = str(v) if v else ""
            xml_data = ET.tostring(root, encoding="utf-8")
            temp_file = os.path.join(WATCH_DIR, f"{metadata['base']}_chunk_{idx}.xml")
            with open(temp_file, "wb") as f:
                f.write(xml_data)

            # Export via POST
            with open(temp_file, "rb") as f:
                xml_content=f.read()
                response = requests.post(
                    url=metadata["url"],
                    headers={
                        "Authorization": f"Bearer {BEARER_TOKEN}",
                        "Content-Type": "application/xml"
                    },
                    data=xml_content
                )
            print(f"📤 Chunk {idx} exported | Status: {response.status_code}")
            print(f"⚠️ Response Status: {response.status_code}")
            print(f"📨 Response Text: {response.text}")
            os.remove(temp_file)

    @task
    def cleanup(context):
        shutil.move(context["metadata"]["file"], os.path.join(PROCESSED_DIR, os.path.basename(context["metadata"]["file"])))
        os.remove(context["metadata"]["lock"])
        print(f"✅ File moved and lock removed: {context['metadata']['file']}")

    # Chain execution
    file_context = find_csv_file()
    metadata_context = extract_metadata(file_context)
    chunks_context = read_csv_chunks(metadata_context)
    export_chunks(chunks_context) >> cleanup(chunks_context)

    
