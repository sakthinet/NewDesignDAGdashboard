from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
import os, json, pandas as pd

CSV_FOLDER = "/opt/airflow/data/incomingcsv/"

def get_runtime_config():
    ctx = get_current_context()
    params = ctx["params"]
    return {
        "schema_name": params.get("Schema Name", "SAP"),
        "default_schema": params.get("Default Schema Name", "SAP"),
        "locale": params.get("Locale", "en-US"),
        "application_name": params.get("Application Name", "MyApp"),
    }

def is_date(val):
    try:
        s = str(val)
        if any(c in s for c in "-/:") and not s.isdigit():
            pd.to_datetime(s, errors="raise")
            return True
    except:
        return False

def is_float(val):
    try:
        return "." in str(val) or not float(val).is_integer()
    except:
        return False

def is_integer(val):
    try:
        int(val)
        return True
    except:
        return False

default_args = {"start_date": datetime(2025, 7, 9)}

with DAG(
    dag_id="IA_CSV_Metadata_Generator",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["InfoArchive", "Metadata Generator"],
    params={
        "Application Name": "MyApp",
        "Default Schema Name": "SAP",
        "Locale": "en-US",
        "Schema Name": "SAP"
    }
) as dag:

    @task()
    def analyze_and_store_json():
        cfg = get_runtime_config()
        tables = []

        for fn in os.listdir(CSV_FOLDER):
            if not fn.endswith(".csv"):
                continue

            path = os.path.join(CSV_FOLDER, fn)
            states = {}
            record_count = 0

            for chunk in pd.read_csv(path, chunksize=10000, dtype=object):
                record_count += len(chunk)
                if not states:
                    states = {
                        c: {"date": False, "float": False, "string": False, "done": False}
                        for c in chunk.columns
                    }

                for _, row in chunk.iterrows():
                    for col, st in states.items():
                        if st["done"]:
                            continue
                        v = row[col]
                        if pd.isna(v) or v == "":
                            continue
                        if not st["date"] and is_date(v):
                            st.update(date=True, done=True)
                        elif st["date"]:
                            continue
                        elif is_float(v):
                            st["float"] = True
                        elif not is_integer(v):
                            st.update(string=True, done=True)

            col_types = {}
            for col, st in states.items():
                if st["date"]:
                    col_types[col] = "DATE"
                elif st["string"]:
                    col_types[col] = "VARCHAR"
                elif st["float"]:
                    col_types[col] = "FLOAT"
                else:
                    col_types[col] = "INT"

            table_columns = []
            for col in col_types:
                col_type = col_types[col]
                type_length = "255" if col_type == "VARCHAR" else "64"
                table_columns.append({
                    "name": col,
                    "type": col_type,
                    "typeLength": type_length
                })

            tables.append({
                "table_name": os.path.splitext(fn)[0],
                "columns": table_columns,
                "record_count": record_count
            })

        metadata = {**cfg, "tables": tables}
        json_filename = f"{cfg['application_name']}_{cfg['default_schema']}_metadata.json"
        out_path = os.path.join(CSV_FOLDER, json_filename)
        with open(out_path, "w") as f:
            json.dump(metadata, f, indent=2)

    analyze_and_store_json()