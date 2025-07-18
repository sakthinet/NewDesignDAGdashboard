from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime
import xml.etree.ElementTree as ET
import json
import os

# 📂 Folder paths
CSV_FOLDER = "/opt/airflow/data/incomingcsv/"
OUTPUT_FOLDER = "/opt/airflow/data/processedcsv/"

default_args = {"start_date": datetime(2025, 7, 9)}

with DAG(
    dag_id="IA_XML_Generator",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["InfoArchive", "XML Generator"],
    params={
        "Application Name": "MyApp",            # Can be overridden via API call
        "Default Schema Name": "SAP"            # Can be overridden via API call
    }
) as dag:

    @task()
    def generate_xml():
        # 🧠 Get runtime parameters
        ctx = get_current_context()
        app_name = ctx["params"].get("Application Name", "MyApp")
        default_schema = ctx["params"].get("Default Schema Name", "SAP")

        # 📦 JSON file name logic
        json_filename = f"{app_name}_{default_schema}_metadata.json"
        json_path = os.path.join(CSV_FOLDER, json_filename)

        # 🔍 Load metadata from JSON
        with open(json_path, "r") as f:
            cfg = json.load(f)

        # 📝 Prepare output file path
        output_file = os.path.join(
            OUTPUT_FOLDER, f"database-{cfg['application_name']}SqlDb.xml"
        )

        # 🌳 Build XML document
        root = ET.Element("metadata")
        ET.SubElement(root, "defaultSchema").text = cfg["default_schema"]
        ET.SubElement(root, "locale").text = cfg["locale"]

        sml = ET.SubElement(root, "schemaMetadataList")
        sch = ET.SubElement(sml, "schemaMetadata")
        ET.SubElement(sch, "name").text = cfg["schema_name"]
        ET.SubElement(sch, "tableCount").text = str(len(cfg["tables"]))

        tml = ET.SubElement(sch, "tableMetadataList")
        for tbl in cfg["tables"]:
            tbl_node = ET.SubElement(tml, "tableMetadata")
            ET.SubElement(tbl_node, "name").text = tbl["table_name"]
            ET.SubElement(tbl_node, "recordCount").text = str(tbl["record_count"])

            cl = ET.SubElement(tbl_node, "columnList")
            for idx, col in enumerate(tbl["columns"], start=1):
                c = ET.SubElement(cl, "column")
                ET.SubElement(c, "name").text = col["name"]
                ET.SubElement(c, "ordinal").text = str(idx)
                ET.SubElement(c, "type").text = col["type"]
                ET.SubElement(c, "typeLength").text = col["typeLength"]
                ET.SubElement(c, "indexing").text = "VALUE"

        # 💾 Write XML to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n')
            f.write(ET.tostring(root, encoding="unicode"))

    generate_xml()
