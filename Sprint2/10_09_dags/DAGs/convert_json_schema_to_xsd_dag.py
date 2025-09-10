from airflow.decorators import dag, task
from datetime import datetime
import os
import json

INPUT_FOLDER = "/opt/airflow/data/output"
OUTPUT_FOLDER = "/opt/airflow/data/output"
XSD_VERSION = "1.0"

@dag(
    dag_id="convert_json_schema_to_xsd_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["infoarchive", "pdi", "xsd", "json", "convert"],
    description="Convert JSON schema to XSD and save as pdiSchema-urnEasSamplesEnXsd<holdingname>.<version>.xsd"
)
def convert_json_schema_to_xsd_dag(
    application_name: str = "CreditCardStatementsArchive",
    holding_name: str = "creditcardholding",
    xsd_root_element: str = "Records",
    xsd_child_element: str = "Record"
):
    @task()
    def get_json_filename(application_name: str, holding_name: str):
        json_file_name = f"pdiSchema-urnEasSamplesEnXsd{application_name.lower()}-{holding_name.lower()}.{XSD_VERSION}.json"
        json_path = os.path.join(INPUT_FOLDER, json_file_name)
        print(f"[DEBUG] Looking for JSON schema file: {json_path}")
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"JSON schema file not found: {json_path}")
        return json_path

    @task()
    def read_json(json_path: str):
        with open(json_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        return schema

    @task()
    def json_schema_to_xsd(schema: dict, holding_name: str, xsd_root_element: str, xsd_child_element: str):
        def build_xsd_elements(properties, xsd_sequence=None, indent=14):
            xsd_lines = []
            # Use xsd_sequence for order if present
            keys = xsd_sequence if xsd_sequence else list(properties.keys())
            for name in keys:
                prop = properties[name]
                typ = prop.get("type", "string")
                fmt = prop.get("format")
                if typ == "object" and "properties" in prop:
                    xsd_lines.append(' ' * indent + f'<xs:element name="{name}">')
                    xsd_lines.append(' ' * (indent+2) + '<xs:complexType>')
                    xsd_lines.append(' ' * (indent+4) + '<xs:sequence>')
                    xsd_lines += build_xsd_elements(prop["properties"], prop.get("xsd_sequence"), indent+6)
                    xsd_lines.append(' ' * (indent+4) + '</xs:sequence>')
                    xsd_lines.append(' ' * (indent+2) + '</xs:complexType>')
                    xsd_lines.append(' ' * indent + '</xs:element>')
                elif name == "checksum" and "xsd_extension_attributes" in prop:
                    xsd_lines.append(' ' * indent + f'<xs:element name="{name}" minOccurs="0">')
                    xsd_lines.append(' ' * (indent+2) + '<xs:complexType>')
                    xsd_lines.append(' ' * (indent+4) + '<xs:simpleContent>')
                    xsd_lines.append(' ' * (indent+6) + '<xs:extension base="xs:string">')
                    for attr_name, attr_type in prop["xsd_extension_attributes"].items():
                        xsd_lines.append(' ' * (indent+8) + f'<xs:attribute name="{attr_name}" type="{attr_type}" use="required"/>')
                    xsd_lines.append(' ' * (indent+6) + '</xs:extension>')
                    xsd_lines.append(' ' * (indent+4) + '</xs:simpleContent>')
                    xsd_lines.append(' ' * (indent+2) + '</xs:complexType>')
                    xsd_lines.append(' ' * indent + '</xs:element>')
                elif typ == "array" and "items" in prop:
                    item_type = prop["items"].get("type", "string")
                    xsd_type = "xs:string"
                    if item_type == "number":
                        xsd_type = "xs:decimal"
                    elif item_type == "string" and prop["items"].get("format") == "date-time":
                        xsd_type = "xs:dateTime"
                    elif item_type == "string" and prop["items"].get("format") == "date":
                        xsd_type = "xs:date"
                    xsd_lines.append(' ' * indent + f'<xs:element name="{name}" type="{xsd_type}" maxOccurs="unbounded"/>')
                else:
                    if typ == "number":
                        xsd_type = "xs:decimal"
                    elif typ == "string" and fmt == "date-time":
                        xsd_type = "xs:dateTime"
                    elif typ == "string" and fmt == "date":
                        xsd_type = "xs:date"
                    else:
                        xsd_type = "xs:string"
                    xsd_lines.append(' ' * indent + f'<xs:element name="{name}" type="{xsd_type}"/>')
            return xsd_lines
        holding_name_lower = holding_name.lower()
        target_ns = f"urn:eas-samples:en:xsd:{holding_name_lower}.{XSD_VERSION}"
        xsd = [
            f'<xs:schema elementFormDefault="qualified"',
            f'  targetNamespace="{target_ns}"',
            f'  version="{XSD_VERSION}" xmlns:xs="http://www.w3.org/2001/XMLSchema">',
            f'  <xs:element name="{xsd_root_element}">',
            f'    <xs:complexType>',
            f'      <xs:sequence maxOccurs="unbounded">',
            f'        <xs:element name="{xsd_child_element}" >',
            f'          <xs:complexType>',
            f'            <xs:sequence>'
        ]
        # If the child element is an object, build its properties
        child = schema.get("properties", {}).get(xsd_child_element, {})
        child_props = child.get("properties")
        child_seq = child.get("xsd_sequence")
        if child_props:
            xsd += build_xsd_elements(child_props, child_seq)
        else:
            # Fallback: build all top-level properties
            xsd += build_xsd_elements(schema.get("properties", {}), schema.get("xsd_sequence"))
        xsd.append('            </xs:sequence>')
        xsd.append('          </xs:complexType>')
        xsd.append('        </xs:element>')
        xsd.append('      </xs:sequence>')
        xsd.append('    </xs:complexType>')
        xsd.append('  </xs:element>')
        xsd.append('</xs:schema>')
        return "\n".join(xsd)

    @task()
    def write_xsd(xsd_str: str, holding_name: str):
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        xsd_file_name = f"pdiSchema-urnEasSamplesEnXsd{holding_name.lower()}.{XSD_VERSION}.xsd"
        xsd_path = os.path.join(OUTPUT_FOLDER, xsd_file_name)
        with open(xsd_path, "w", encoding="utf-8") as f:
            f.write(xsd_str)
        print(f"✅ XSD written to {xsd_path}")
        return xsd_path

    json_path = get_json_filename(application_name, holding_name)
    schema = read_json(json_path)
    xsd_str = json_schema_to_xsd(schema, holding_name, xsd_root_element, xsd_child_element)
    write_xsd(xsd_str, holding_name)

convert_json_schema_to_xsd_dag = convert_json_schema_to_xsd_dag()
