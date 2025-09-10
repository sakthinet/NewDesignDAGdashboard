from airflow.decorators import dag, task
from datetime import datetime
import os, csv, re, mimetypes, hashlib, base64, json
from collections import Counter, OrderedDict

CSV_FOLDER = "/opt/airflow/data/incomingcsv/"
ATTACHMENT_FOLDER = "/opt/airflow/data/attachments/"
OUTPUT_FOLDER = "/opt/airflow/data/output/"
XSD_VERSION = "1.0"
RESERVED_XSD_KEYWORDS = {"language", "type", "schema", "element", "complexType", "sequence"}

@dag(
    dag_id="generate_pdi_schema_and_attachments_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["infoarchive", "pdi", "xsd", "schema", "attachments"],
    description="Expert: Generate InfoArchive PDI XSD schema and attachment info as one JSON from CSV metadata"
)
def generate_pdi_schema_and_attachments_dag(
    csv_filename: str = "CreditCardTransactions.csv",
    application_name: str = "CreditCardStatementsArchive",
    holding_name: str = "creditcardholding",
    xsd_root_element: str = "Records",
    xsd_child_element: str = "Record"
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
    def infer_xsd_types(data: dict):
        headers = data["headers"]
        rows = data["rows"]
        types = {}
        dt_regex = re.compile(r"^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}$")
        date_regex = re.compile(r"^\\d{4}-\\d{2}-\\d{2}$")
        int_regex = re.compile(r"^\\d+$")
        float_regex = re.compile(r"^\\d+\\.\\d+$")
        for idx, col in enumerate(headers):
            orig_col = col
            if col.lower() in RESERVED_XSD_KEYWORDS:
                col = f"{col}_field"
            values = [row[idx] for row in rows if len(row) > idx and row[idx].strip()]
            if idx == len(headers) - 1:
                types[col] = "attachment"
                continue
            type_counter = Counter()
            for v in values:
                if dt_regex.match(v):
                    type_counter["xs:dateTime"] += 1
                elif date_regex.match(v):
                    type_counter["xs:date"] += 1
                elif int_regex.match(v):
                    type_counter["xs:long"] += 1
                elif float_regex.match(v):
                    type_counter["xs:decimal"] += 1
                else:
                    type_counter["xs:string"] += 1
            for t in ["xs:dateTime", "xs:date", "xs:long", "xs:decimal", "xs:string"]:
                if type_counter[t] > 0:
                    types[col] = t
                    break
            else:
                types[col] = "xs:string"
        return {"headers": headers, "types": types}

    @task()
    def build_xsd(inferred: dict, holding_name: str, xsd_root_element: str, xsd_child_element: str):
        headers = inferred["headers"]
        types = inferred["types"]
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
        for col in headers[:-1]:
            col_name = col if col.lower() not in RESERVED_XSD_KEYWORDS else f"{col}_field"
            xsd_type = types[col_name]
            if xsd_type in {"xs:dateTime", "xs:date", "xs:long", "xs:decimal"}:
                xsd.append(f'              <xs:element name="{col_name}" type="{xsd_type}"/>')
            else:
                xsd.append(f'              <xs:element name="{col_name}" type="xs:string"/>')
        # Attachments block
        xsd.append('              <xs:element name="Attachments" minOccurs="0">')
        xsd.append('                <xs:complexType>')
        xsd.append('                  <xs:sequence maxOccurs="unbounded" minOccurs="0">')
        xsd.append('                    <xs:element name="Attachment">')
        xsd.append('                      <xs:complexType>')
        xsd.append('                        <xs:sequence>')
        xsd.append('                          <xs:element name="mimetype" type="xs:string" minOccurs="0"/>')
        xsd.append('                          <xs:element name="Size" type="xs:long" minOccurs="0"/>')
        xsd.append('                          <xs:element name="EASFile" type="xs:string" minOccurs="0"/>')
        xsd.append('                          <xs:element name="checksum" minOccurs="0">')
        xsd.append('                            <xs:complexType>')
        xsd.append('                              <xs:simpleContent>')
        xsd.append('                                <xs:extension base="xs:string">')
        xsd.append('                                  <xs:attribute name="algorithm" type="xs:string" use="required"/>')
        xsd.append('                                  <xs:attribute name="encoding" type="xs:string" use="required"/>')
        xsd.append('                                </xs:extension>')
        xsd.append('                              </xs:simpleContent>')
        xsd.append('                            </xs:complexType>')
        xsd.append('                          </xs:element>')
        xsd.append('                          <xs:element name="FilePath" type="xs:string" minOccurs="0"/>')
        xsd.append('                        </xs:sequence>')
        xsd.append('                      </xs:complexType>')
        xsd.append('                    </xs:element>')
        xsd.append('                  </xs:sequence>')
        xsd.append('                </xs:complexType>')
        xsd.append('              </xs:element>')
        xsd.append('            </xs:sequence>')
        xsd.append('          </xs:complexType>')
        xsd.append('        </xs:element>')
        xsd.append('      </xs:sequence>')
        xsd.append('    </xs:complexType>')
        xsd.append('  </xs:element>')
        xsd.append('</xs:schema>')
        return "\\n".join(xsd)

    @task()
    def build_combined_json(data: dict, xsd_str: str, application_name: str, holding_name: str):
        # Only include the XSD schema in the output JSON
        combined = {
            "schema": xsd_str
        }
        return combined

    @task()
    def write_combined_json(combined: dict, application_name: str, holding_name: str):
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        json_file_name = f"pdiSchema-urnEasSamplesEnXsd{application_name.lower()}-{holding_name.lower()}.{XSD_VERSION}.json"
        json_path = os.path.join(OUTPUT_FOLDER, json_file_name)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2)
        print(f"✅ Combined JSON written to {json_path}")
        return json_path

    @task()
    def xsd_to_json_schema(xsd_str: str, xsd_root_element: str, xsd_child_element: str):
        import xml.etree.ElementTree as ET
        from collections import OrderedDict
        def process_element(elem, ns):
            schema = {"type": "object", "properties": OrderedDict(), "xsd_sequence": []}
            sequence = elem.find('xs:complexType/xs:sequence', ns)
            if sequence is not None:
                for child in sequence.findall('xs:element', ns):
                    name = child.get('name')
                    typ = child.get('type', 'xs:string')
                    schema["xsd_sequence"].append(name)
                    # If child has its own complexType, recurse
                    if child.find('xs:complexType', ns) is not None:
                        # Special handling for checksum extension attributes
                        if name == "checksum":
                            ext = child.find('xs:complexType/xs:simpleContent/xs:extension', ns)
                            prop = {"type": "string", "xsd_extension_attributes": OrderedDict()}
                            if ext is not None:
                                for attr in ext.findall('xs:attribute', ns):
                                    attr_name = attr.get('name')
                                    attr_type = attr.get('type')
                                    prop["xsd_extension_attributes"][attr_name] = attr_type
                            schema["properties"][name] = prop
                        else:
                            schema["properties"][name] = process_element(child, ns)
                    else:
                        if typ in ['xs:long', 'xs:decimal', 'xs:int', 'xs:integer', 'xs:float', 'xs:double']:
                            jtype = "number"
                        elif typ in ['xs:date', 'xs:dateTime']:
                            jtype = "string"
                            fmt = "date-time" if typ == 'xs:dateTime' else "date"
                        else:
                            jtype = "string"
                            fmt = None
                        prop = {"type": jtype}
                        if typ in ['xs:date', 'xs:dateTime']:
                            prop["format"] = fmt
                        schema["properties"][name] = prop
            return schema
        # Ensure XSD string is well-formed XML
        if isinstance(xsd_str, list):
            xsd_str_clean = '\n'.join(xsd_str)
        else:
            xsd_str_clean = xsd_str.replace('\\n', '\n')
        try:
            root = ET.fromstring(xsd_str_clean)
        except ET.ParseError as e:
            print(f"❌ XML ParseError: {e}\nXSD string: {xsd_str_clean[:200]}")
            raise
        ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
        for elem in root.findall('.//xs:element', ns):
            if elem.get('name') == xsd_root_element:
                json_schema = process_element(elem, ns)
                json_schema["$schema"] = "http://json-schema.org/draft-07/schema#"
                json_schema["title"] = xsd_root_element
                return json_schema
        raise ValueError(f"Root element '{xsd_root_element}' not found in XSD.")

    @task()
    def write_json_schema(json_schema: dict, application_name: str, holding_name: str):
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        json_file_name = f"pdiSchema-urnEasSamplesEnXsd{application_name.lower()}-{holding_name.lower()}.{XSD_VERSION}.json"
        json_path = os.path.join(OUTPUT_FOLDER, json_file_name)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_schema, f, indent=2)
        print(f"✅ JSON schema written to {json_path}")
        return json_path

    # DAG orchestration
    csv_data = read_csv(csv_filename)
    inferred = infer_xsd_types(csv_data)
    xsd_str = build_xsd(inferred, holding_name, xsd_root_element, xsd_child_element)
    json_schema = xsd_to_json_schema(xsd_str, xsd_root_element, xsd_child_element)
    write_json_schema(json_schema, application_name, holding_name)


generate_pdi_schema_and_attachments_dag = generate_pdi_schema_and_attachments_dag()