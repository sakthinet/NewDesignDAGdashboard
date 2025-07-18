from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
import os
from airflow.operators.python import get_current_context

CSV_FOLDER = "/opt/airflow/data/incomingcsv/"
OUTPUT_FOLDER = "/opt/airflow/data/processedcsv/"

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

default_args = {
    "start_date": datetime(2025, 7, 9)
}

with DAG(
    dag_id="IASchemaGenerator",
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
    def analyze_all_csvs():
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
                    states = {c: {"date": False, "float": False, "string": False, "done": False}
                              for c in chunk.columns}
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

            tables.append({
                "table_name": os.path.splitext(fn)[0],
                "columns": [(c, col_types[c]) for c in col_types.keys()],
                "record_count": record_count
            })

        return tables

    @task()
    def generate_metadata_xml(table_infos):
        cfg = get_runtime_config()
        name = cfg["application_name"]
        out = os.path.join(OUTPUT_FOLDER, f"database-{name}SqlDb.xml")

        root = ET.Element("metadata")
        ET.SubElement(root, "defaultSchema").text = cfg["default_schema"]
        ET.SubElement(root, "locale").text = cfg["locale"]

        sml = ET.SubElement(root, "schemaMetadataList")
        sch = ET.SubElement(sml, "schemaMetadata")
        ET.SubElement(sch, "name").text = cfg["schema_name"]
        ET.SubElement(sch, "tableCount").text = str(len(table_infos))

        tml = ET.SubElement(sch, "tableMetadataList")
        for info in table_infos:
            tbl = ET.SubElement(tml, "tableMetadata")
            ET.SubElement(tbl, "name").text = info["table_name"]
            ET.SubElement(tbl, "recordCount").text = str(info["record_count"])

            cl = ET.SubElement(tbl, "columnList")
            for idx, (col, typ) in enumerate(info["columns"], start=1):
                c = ET.SubElement(cl, "column")
                ET.SubElement(c, "name").text = col
                ET.SubElement(c, "ordinal").text = str(idx)
                ET.SubElement(c, "type").text = typ
                ET.SubElement(c, "typeLength").text = "255" if typ == "VARCHAR" else "64"
                ET.SubElement(c, "indexing").text = "VALUE"

        xml_str = ET.tostring(root, encoding="unicode")
        with open(out, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
            f.write(xml_str)

        return True

    @task()
    def collect_oob_metrics(ti=None):
        ctx = get_current_context()
        dr = ctx["dag_run"]
        dag_id = dr.dag_id
        run_id = dr.run_id
        exec_dt = dr.logical_date
        xml_status = ti.xcom_pull(task_ids="generate_metadata_xml")
        note = "XML successfully generated." if xml_status else "XML not generated yet."

        sql = """
        INSERT INTO dag_run_metrics (
            dag_id, run_id, execution_date, notes
        )
        VALUES (
            %(dag_id)s, %(run_id)s, %(execution_date)s, %(notes)s
        )
        """
        parameters = {
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": exec_dt,
            "notes": note,
        }
        hook = PostgresHook(postgres_conn_id="metrics_db")
        hook.run(sql, parameters=parameters)

    csv_summary = analyze_all_csvs()
    generate_xml = generate_metadata_xml(csv_summary)
    report_metrics = collect_oob_metrics()

    csv_summary >> generate_xml >> report_metrics
