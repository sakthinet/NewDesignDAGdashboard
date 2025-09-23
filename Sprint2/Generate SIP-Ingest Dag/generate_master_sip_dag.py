from airflow.decorators import dag, task
from datetime import datetime
import os
import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom
import random
import uuid
import zipfile

CSV_FOLDER = "/opt/airflow/data/incomingcsv/"
OUTPUT_FOLDER = "/opt/airflow/data/output"
ATTACHMENTS_FOLDER = CSV_FOLDER  # Attachments are now in incomingcsv
SIP_ZIP_FOLDER = "/opt/airflow/data/sipzips"

@dag(
    dag_id="generate_sip_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["infoarchive", "sip", "pdi", "xml", "master"],
    description="Master DAG to generate InfoArchive SIP zip with eas_sip.xml, eas_pdi.xml, and referenced attachments"
)
def generate_sip_dag(
    csv_filename: str = "CreditCardTransactions.csv",
    holding_name: str = "creditcardholding",
    application_name: str = "CreditCardStatementsArchive",
    root_element: str = "Records",
    child_element: str = "Record",
    records_per_sip: int = 20,  # Dynamic input: number of records per SIP
    max_sip_size_kb: int = 102400  # Dynamic input: max SIP size in KB
):
    @task()
    def read_csv(csv_filename: str):
        csv_path = os.path.join(CSV_FOLDER, csv_filename)
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
        return {"headers": headers, "rows": rows}

    @task()
    def process_rows(data: dict):
        headers = data["headers"]
        rows = data["rows"]
        attachment_headers = [h for h in headers if 'attachment' in h.lower()]
        meta_headers = [h for h in headers if 'attachment' not in h.lower()]
        return {
            "headers": headers,
            "rows": rows,
            "meta_headers": meta_headers,
            "attachment_headers": attachment_headers
        }


    # --- Helper functions (not @task) ---
    def build_pdi_xml(processed: dict, holding_name: str, root_element: str, child_element: str):
        import mimetypes, hashlib, base64
        holding_name_lower = holding_name.lower()
        namespace = f"urn:eas-samples:en:xsd:{holding_name_lower}.1.0"
        root = ET.Element(root_element, xmlns=namespace)
        for row in processed["rows"]:
            if not any(row) or len(row) < len(processed["headers"]):
                continue
            record_elem = ET.SubElement(root, child_element)
            for h in processed["meta_headers"]:
                idx = processed["headers"].index(h)
                record_elem_h = ET.SubElement(record_elem, h)
                record_elem_h.text = row[idx].strip()
            attachments_elem = ET.SubElement(record_elem, "Attachments")
            for h in processed["attachment_headers"]:
                idx = processed["headers"].index(h)
                path_val = row[idx].strip()
                if path_val:
                    attachment_elem = ET.SubElement(attachments_elem, "Attachment")
                    file_name = os.path.basename(path_val)
                    abs_path = os.path.join(ATTACHMENTS_FOLDER, file_name)
                    mimetype, _ = mimetypes.guess_type(abs_path)
                    size = os.path.getsize(abs_path) if os.path.exists(abs_path) else None
                    checksum_val = None
                    if os.path.exists(abs_path):
                        with open(abs_path, "rb") as f:
                            checksum_val = base64.b64encode(hashlib.sha256(f.read()).digest()).decode()
                    ET.SubElement(attachment_elem, "mimetype").text = mimetype or "application/octet-stream"
                    ET.SubElement(attachment_elem, "Size").text = str(size) if size is not None else ""
                    ET.SubElement(attachment_elem, "EASFile").text = file_name
                    checksum_elem = ET.SubElement(attachment_elem, "checksum", algorithm="SHA-256", encoding="base64")
                    checksum_elem.text = checksum_val if checksum_val else ""
                    ET.SubElement(attachment_elem, "FilePath").text = f"/opt/airflow/data/incomingcsv/{file_name}"
        return ET.tostring(root, encoding="utf-8").decode("utf-8")

    def prettify_xml(xml_str: str):
        parsed = minidom.parseString(xml_str)
        return parsed.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")

    def write_to_file(xml_str: str, output_path: str):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(xml_str)
        print(f"✅ XML written to {output_path}")
        return output_path

    def get_attachment_paths_from_csv(data: dict):
        headers = data["headers"]
        rows = data["rows"]
        attachment_paths = set()
        for row in rows:
            for h in headers:
                if 'attachment' in h.lower():
                    idx = headers.index(h)
                    if len(row) > idx:
                        path_val = row[idx].strip()
                        if path_val:
                            attachment_paths.add(os.path.basename(path_val))
        if not attachment_paths:
            raise ValueError("No attachments found in CSV.")
        print("Attachments to add:", attachment_paths)
        return list(attachment_paths)

    def generate_metadata(application_name: str, holding_name: str):
        return {
            "uuid": str(random.randint(1000000000, 9999999999)),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "application_name": application_name,
            "holding_name": holding_name,
            "holding_name_lower": holding_name.lower()
        }

    def count_aius_in_pdi_xml(pdi_xml_path: str, child_element: str):
        try:
            print(f"[DEBUG] child_element value: {child_element}")
            tree = ET.parse(pdi_xml_path)
            root = tree.getroot()
            if root.tag.startswith('{'):
                ns_uri = root.tag.split('}')[0].strip('{')
                ns = {'ns': ns_uri}
                search_tag = f'.//ns:{child_element}'
            else:
                ns = {}
                search_tag = f'.//{child_element}'
            found = root.findall(search_tag, ns)
            print(f"[DEBUG] Found {len(found)} elements for tag '{child_element}' with search_tag '{search_tag}' and ns {ns}")
            if not found:
                raise ValueError(f"No AIUs found in PDI XML. Searched for tag: '{child_element}' with namespace {ns}. Found: {found}")
            return len(found)
        except Exception as e:
            print(f"❌ Error in count_aius_in_pdi_xml: {e}")
            raise

    def build_sip_xml(metadata: dict, aiu_count: int):
        sip = ET.Element("sip", xmlns="urn:x-emc:ia:schema:sip:1.0")
        dss = ET.SubElement(sip, "dss")
        ET.SubElement(dss, "holding").text = metadata["holding_name"]
        ET.SubElement(dss, "id").text = metadata["uuid"]
        ET.SubElement(dss, "pdi_schema").text = f"urn:eas-samples:en:xsd:{metadata['holding_name_lower']}.1.0"
        ET.SubElement(dss, "production_date").text = metadata["timestamp"]
        ET.SubElement(dss, "base_retention_date").text = metadata["timestamp"]
        ET.SubElement(dss, "producer").text = metadata["application_name"]
        ET.SubElement(dss, "entity").text = metadata["application_name"]
        ET.SubElement(dss, "priority").text = "0"
        ET.SubElement(dss, "application").text = metadata["application_name"]
        ET.SubElement(sip, "production_date").text = metadata["timestamp"]
        ET.SubElement(sip, "seqno").text = "1"
        ET.SubElement(sip, "is_last").text = "true"
        ET.SubElement(sip, "aiu_count").text = str(aiu_count)
        ET.SubElement(sip, "page_count").text = "0"
        return ET.tostring(sip, encoding="utf-8").decode("utf-8")

    def validate_pdi_xml_against_schema(pdi_xml_path: str, holding_name: str):
        try:
            import lxml.etree as LET
            schema_path = os.path.join(OUTPUT_FOLDER, f"pdiSchema-urnEasSamplesEnXsd{holding_name}.1.0.xsd")
            if not os.path.exists(schema_path):
                raise FileNotFoundError(f"Schema not found: {schema_path}")
            xml_doc = LET.parse(pdi_xml_path)
            xmlschema_doc = LET.parse(schema_path)
            xmlschema = LET.XMLSchema(xmlschema_doc)
            is_valid = xmlschema.validate(xml_doc)
            if not is_valid:
                raise ValueError("PDI XML does not validate against schema.")
            print(f"✅ PDI XML validation result: {is_valid}")
            return is_valid
        except Exception as e:
            print(f"❌ Error in validate_pdi_xml_against_schema: {e}")
            raise

    def create_sip_zip(
        holding_name: str,
        eas_sip_xml: str,
        eas_pdi_xml: str,
        attachments: list,
        attachments_folder: str,
        output_folder: str
    ):
        try:
            unique_id = str(uuid.uuid4())
            zip_name = f"{holding_name}-{unique_id}.zip"
            zip_path = os.path.join(output_folder, zip_name)
            max_size = 2 * 1024 * 1024 * 1024  # 2GB
            files_to_add = [
                (eas_pdi_xml, os.path.basename(eas_pdi_xml)),
                (eas_sip_xml, os.path.basename(eas_sip_xml)),
            ]
            for att_name in attachments:
                att_path = os.path.join(attachments_folder, att_name)
                if os.path.exists(att_path):
                    files_to_add.append((att_path, att_name))
                else:
                    print(f"❌ Attachment not found: {att_path}")
                    raise FileNotFoundError(f"Attachment not found: {att_path}")
            total_size = 0
            os.makedirs(output_folder, exist_ok=True)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for src, arcname in files_to_add:
                    file_size = os.path.getsize(src)
                    if total_size + file_size > max_size:
                        print(f"❌ Skipping {arcname}: would exceed 2GB limit.")
                        continue
                    zf.write(src, arcname)
                    total_size += file_size
            print(f"✅ SIP ZIP created: {zip_path} ({total_size/1024/1024:.2f} MB)")
            return zip_path
        except Exception as e:
            print(f"❌ Error in create_sip_zip: {e}")
            raise

    # ----------- DAG Orchestration with Correct Order and Attachments -----------
    # 1. Create eas_pdi.xml first


    @task()
    def package_sips(processed, holding_name, application_name, root_element, child_element, records_per_sip, max_sip_size_kb):
        import os
        headers = processed['headers']
        rows = processed['rows']
        total_records = len(rows)
        # --- Study: Calculate total volume of all files to be packaged ---
        all_attachments = get_attachment_paths_from_csv({'headers': headers, 'rows': rows})
        total_attachment_size = 0
        for att in all_attachments:
            att_path = os.path.join(ATTACHMENTS_FOLDER, att)
            if os.path.exists(att_path):
                total_attachment_size += os.path.getsize(att_path)
        print(f"[STUDY] Total records: {total_records}")
        print(f"[STUDY] Total unique attachments: {len(all_attachments)}")
        print(f"[STUDY] Total attachment size: {total_attachment_size // 1024} KB")
        # --- End Study ---

        # --- Optimization: Estimate record+attachment size and cluster records ---
        # Estimate size for each record (attachments + XML overhead)
        record_sizes = []
        for i, row in enumerate(rows):
            size = 0
            for h in headers:
                if 'attachment' in h.lower():
                    idx = headers.index(h)
                    if len(row) > idx:
                        path_val = row[idx].strip()
                        if path_val:
                            att_path = os.path.join(ATTACHMENTS_FOLDER, os.path.basename(path_val))
                            if os.path.exists(att_path):
                                size += os.path.getsize(att_path)
            # Add a fixed XML overhead per record (estimate 2 KB)
            size += 2 * 1024
            record_sizes.append(size)

        # Greedy bin-packing: cluster records into SIPs by size
        clusters = []
        current_cluster = []
        current_size = 0
        for idx, (row, rsize) in enumerate(zip(rows, record_sizes)):
            if (current_size + rsize > max_sip_size_kb * 1024) or (len(current_cluster) >= records_per_sip):
                if current_cluster:
                    clusters.append(current_cluster)
                current_cluster = []
                current_size = 0
            current_cluster.append(row)
            current_size += rsize
        if current_cluster:
            clusters.append(current_cluster)

        print(f"[CLUSTER] Total SIPs to create: {len(clusters)}")

        sip_zip_paths = []
        for sip_idx, sip_rows in enumerate(clusters):
            sip_processed = {
                'headers': headers,
                'rows': sip_rows,
                'meta_headers': processed['meta_headers'],
                'attachment_headers': processed['attachment_headers']
            }
            temp_sip_folder = os.path.join(OUTPUT_FOLDER, f"sip_{sip_idx}_tmp")
            os.makedirs(temp_sip_folder, exist_ok=True)
            raw_pdi_xml = build_pdi_xml(sip_processed, holding_name, root_element, child_element)
            pretty_pdi_xml = prettify_xml(raw_pdi_xml)
            pdi_xml_path = os.path.join(temp_sip_folder, "eas_pdi.xml")
            pdi_written = write_to_file(pretty_pdi_xml, pdi_xml_path)
            aiu_count = count_aius_in_pdi_xml(pdi_written, child_element)
            pdi_valid = validate_pdi_xml_against_schema(pdi_written, holding_name)
            sip_csv_data = {'headers': headers, 'rows': sip_rows}
            attachments = get_attachment_paths_from_csv(sip_csv_data)
            metadata = generate_metadata(application_name, holding_name)
            raw_sip_xml = build_sip_xml(metadata, aiu_count)
            pretty_sip_xml = prettify_xml(raw_sip_xml)
            sip_xml_path = os.path.join(temp_sip_folder, "eas_sip.xml")
            sip_written = write_to_file(pretty_sip_xml, sip_xml_path)
            zip_path = create_sip_zip(
                holding_name=f"{holding_name}_{sip_idx}",
                eas_sip_xml=sip_xml_path,
                eas_pdi_xml=pdi_written,
                attachments=attachments,
                attachments_folder=ATTACHMENTS_FOLDER,
                output_folder=SIP_ZIP_FOLDER
            )
            if os.path.exists(zip_path):
                print(f"[CLUSTER] SIP {sip_idx} size: {os.path.getsize(zip_path) // 1024} KB, records: {len(sip_rows)}")
            # Clean up temp folder after packaging
            try:
                for f in os.listdir(temp_sip_folder):
                    os.remove(os.path.join(temp_sip_folder, f))
                os.rmdir(temp_sip_folder)
            except Exception:
                pass
            sip_zip_paths.append(zip_path)
        return sip_zip_paths

    csv_data = read_csv(csv_filename)
    processed = process_rows(csv_data)
    sip_zip_paths = package_sips(
        processed,
        holding_name,
        application_name,
        root_element,
        child_element,
        records_per_sip,
        max_sip_size_kb
    )

generate_sip_dag = generate_sip_dag()