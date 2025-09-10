# dags/IA_CSV_to_PDI_ObjectSchema.py
from airflow.decorators import dag, task
try:
    from airflow.decorators import get_current_context
except Exception:
    from airflow.operators.python import get_current_context

from datetime import datetime
import os, csv, re, json, hashlib, base64, mimetypes

CSV_FOLDER = "/opt/airflow/data/incomingcsv/"
ATTACHMENT_FOLDER = "/opt/airflow/data/attachments/"
OUTPUT_FOLDER = "/opt/airflow/data/output/"
SCHEMA_VERSION = "1.0"

# Simple type inference
ISO_DT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
INT_RE   = re.compile(r"^[+-]?\d+$")
FLOAT_RE = re.compile(r"^[+-]?\d+\.\d+$")

def infer_type(values):
    score = {"date-time": 0, "date": 0, "number": 0, "string": 0}
    for v in values:
        if v is None: 
            continue
        s = f"{v}".strip()
        if not s:
            continue
        if ISO_DT.match(s): score["date-time"] += 1
        elif ISO_DATE.match(s): score["date"] += 1
        elif INT_RE.match(s) or FLOAT_RE.match(s): score["number"] += 1
        else: score["string"] += 1
    for t in ["date-time","date","number","string"]:
        if score[t] > 0: return t
    return "string"

def split_paths(cell: str):
    if not cell: return []
    s = f"{cell}".strip()
    if not s: return []
    if ";" in s: parts = s.split(";")
    elif "|" in s: parts = s.split("|")
    else: parts = [s]
    return [p.strip() for p in parts if p.strip()]

def file_metadata(path: str):
    """
    Build full attachment metadata, computing size + sha-256 checksum.
    Relative paths are resolved against ATTACHMENT_FOLDER.
    """
    if not path:
        return {
            "mimetype": "application/octet-stream",
            "Size": 0,
            "EASFile": None,
            "checksum": None,
            "FilePath": path or ""
        }
    fpath = path if os.path.isabs(path) else os.path.join(ATTACHMENT_FOLDER, path)
    mime, _ = mimetypes.guess_type(fpath)
    meta = {
        "mimetype": mime or "application/octet-stream",
        "Size": 0,
        "EASFile": None,
        "checksum": None,
        "FilePath": path
    }
    if not os.path.isfile(fpath):
        return meta
    total = 0
    sha = hashlib.sha256()
    with open(fpath, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk: break
            total += len(chunk)
            sha.update(chunk)
    meta["Size"] = total
    digest_b64 = base64.b64encode(sha.digest()).decode("ascii")
    meta["checksum"] = digest_b64
    meta["EASFile"] = f"ea_{sha.hexdigest()}"
    return meta

@dag(
    dag_id="csv_to_pdi_object_schema_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["csv","json","schema","pdi","data"],
    description="CSV → PDI object-style schema (Records/Record) + data file; Attachments as object with inner Attachment list.",
    params={
        "csv_filename": "CreditCardTransactions.csv",
        "application_name": "CreditCardStatementsArchive",
        "holding_name": "creditcardholding",
        # Optional: exact attachments column name (case-insensitive). If empty, auto-detects 'Attachments' or 'Attachment'.
        "attachment_column_name": ""
    }
)
def csv_to_pdi_object_schema_dag():

    @task()
    def read_csv():
        ctx = get_current_context()
        csv_filename = ctx["params"]["csv_filename"]
        csv_path = os.path.join(CSV_FOLDER, csv_filename)
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
        return {"headers": headers, "rows": rows}

    @task()
    def build_schema_obj(data: dict):
        """
        Build EXACT schema shape you requested. Attachments only included if present in CSV.
        """
        ctx = get_current_context()
        holding = ctx["params"]["holding_name"]
        preferred = (ctx["params"].get("attachment_column_name") or "").strip().lower()

        headers = data["headers"]
        rows = data["rows"]

        # Locate attachments header (if any)
        attach_header = None
        if preferred:
            for h in headers:
                if h.strip().lower() == preferred:
                    attach_header = h
                    break
        if attach_header is None:
            for h in headers:
                if h.strip().lower() in ("attachments", "attachment"):
                    attach_header = h
                    break

        # Collect values for type inference (we do NOT use the attachments column for inference)
        infer_headers = [h for h in headers if h != attach_header] if attach_header else headers[:]
        col_values = {h: [] for h in infer_headers}
        for row in rows:
            for i, h in enumerate(headers):
                if h in col_values:
                    col_values[h].append(row[i] if i < len(row) else None)

        record_properties = {}
        record_order = []

        for h in headers:
            if attach_header is not None and h == attach_header:
                # Attachments as object with inner Attachment object (ordered)
                attachment_inner_order = ["mimetype", "Size", "EASFile", "checksum", "FilePath"]
                attachments_obj = {
                    "type": "object",
                    "properties": {
                        "Attachment": {
                            "type": "object",
                            "properties": {
                                "Size": {"type": "number"},
                                "EASFile": {"type": "string"},
                                "FilePath": {"type": "string"},
                                "checksum": {
                                    "type": "string",
                                    "xsd_extension_attributes": {
                                        "encoding": "xs:string",
                                        "algorithm": "xs:string"
                                    }
                                },
                                "mimetype": {"type": "string"}
                            },
                            "xsd_sequence": attachment_inner_order
                        }
                    },
                    "xsd_sequence": ["Attachment"]
                }
                record_properties[h] = attachments_obj
                record_order.append(h)
            else:
                t = infer_type(col_values[h]) if h in col_values else "string"
                if t == "number":
                    record_properties[h] = {"type": "number"}
                elif t == "date":
                    record_properties[h] = {"type": "string", "format": "date"}
                elif t == "date-time":
                    record_properties[h] = {"type": "string", "format": "date-time"}
                else:
                    record_properties[h] = {"type": "string"}
                record_order.append(h)

        schema = {
            "type": "object",
            "title": "Records",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": {
                "Record": {
                    "type": "object",
                    "properties": record_properties,
                    "xsd_sequence": record_order
                }
            },
            "xsd_sequence": ["Record"]
        }
        return schema

    @task()
    def write_schema(schema: dict):
        ctx = get_current_context()
        app = ctx["params"]["application_name"].lower()
        holding = ctx["params"]["holding_name"].lower()
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        out = os.path.join(
            OUTPUT_FOLDER,
            f"pdiSchema-urnEasSamplesEnXsd{app}-{holding}.{SCHEMA_VERSION}.json"
        )
        with open(out, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        print(f"✅ Schema written to {out}")
        return out

    @task()
    def csv_rows_to_pdi_object_data(data: dict):
        """
        Build data payload:
        {
          "Records": {
            "Record": [ { row1... }, { row2... }, ... ]
          }
        }
        Attachments column (if present) becomes:
        "Attachments": { "Attachment": [ { ...file meta... }, ... ] }
        """
        ctx = get_current_context()
        headers = data["headers"]
        rows = data["rows"]
        preferred = (ctx["params"].get("attachment_column_name") or "").strip().lower()

        # Find attachments column
        attach_header = None
        if preferred:
            for h in headers:
                if h.strip().lower() == preferred:
                    attach_header = h; break
        if attach_header is None:
            for h in headers:
                if h.strip().lower() in ("attachments", "attachment"):
                    attach_header = h; break

        records = []
        for row in rows:
            rec = {}
            for i, h in enumerate(headers):
                val = row[i] if i < len(row) else None
                if attach_header and h == attach_header:
                    paths = split_paths(val)
                    # Convert to object with "Attachment" list
                    rec[h] = {
                        "Attachment": [file_metadata(p) for p in paths]
                    }
                else:
                    rec[h] = val
            records.append(rec)

        return {"Records": {"Record": records}}

    @task()
    def write_data(payload: dict):
        ctx = get_current_context()
        app = ctx["params"]["application_name"].lower()
        holding = ctx["params"]["holding_name"].lower()
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        out = os.path.join(OUTPUT_FOLDER, f"data-{app}-{holding}.{SCHEMA_VERSION}.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f"✅ Data written to {out}")
        return out

    # Orchestration
    d = read_csv()
    s = build_schema_obj(d)
    write_schema(s)
    payload = csv_rows_to_pdi_object_data(d)
    write_data(payload)

csv_to_pdi_object_schema_dag = csv_to_pdi_object_schema_dag()
