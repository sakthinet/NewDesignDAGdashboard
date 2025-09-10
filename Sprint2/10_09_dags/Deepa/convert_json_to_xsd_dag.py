# /opt/airflow/dags/convert_pdi_object_schema_to_xsd_dynamic_outer.py
from airflow.decorators import dag, task
try:
    from airflow.decorators import get_current_context
except Exception:
    from airflow.operators.python import get_current_context

from datetime import datetime
import os, json
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
trace.get_tracer_provider().add_span_processor(span_processor)


INPUT_FOLDER  = "/opt/airflow/data/output"
OUTPUT_FOLDER = "/opt/airflow/data/output"
XSD_VERSION   = "1.0"

# Keep these as strings in XSD regardless of JSON typing
ALWAYS_STRING = {"CustomerID", "CreditCardNumber"}

@dag(
    dag_id="convert_pdi_object_schema_to_xsd_dynamic_outer_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["infoarchive","pdi","xsd","convert"],
    description="Convert PDI JSON schema to XSD. Outer attachments element name comes from JSON (CSV header).",
    params={
        "application_name": "CreditCardStatementsArchive",
        "holding_name": "creditcardholding",
        "xsd_root_element": "Records",
        "xsd_child_element": "Record"
    }
)
def convert_pdi_object_schema_to_xsd_dynamic_outer_dag():

    @task()
    def get_json_filename() -> str:
        with tracer.start_as_current_span("get_json_filename"):
            ctx = get_current_context()
            app = ctx["params"]["application_name"].lower()
            holding = ctx["params"]["holding_name"].lower()
            fname = f"pdiSchema-urnEasSamplesEnXsd{app}-{holding}.{XSD_VERSION}.json"
            path = os.path.join(INPUT_FOLDER, fname)
            if not os.path.exists(path):
                raise FileNotFoundError(f"JSON schema file not found: {path}")
            return path

    @task()
    def read_json(json_path: str) -> dict:
        with tracer.start_as_current_span("read_json"):
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
    
    @task()
    def json_schema_to_xsd(schema: dict) -> str:
        with tracer.start_as_current_span("json_schema_to_xsd"):
            ctx = get_current_context()
            holding = ctx["params"]["holding_name"]
            root_el = ctx["params"]["xsd_root_element"]
            child_el = ctx["params"]["xsd_child_element"]

        def xs_type(jtype: str | None, fmt: str | None) -> str:
            if jtype == "number": return "xs:decimal"
            if jtype == "string" and fmt == "date-time": return "xs:dateTime"
            if jtype == "string" and fmt == "date": return "xs:date"
            return "xs:string"

        def build_checksum(indent: int) -> list[str]:
            pad = " " * indent
            return [
                f'{pad}<xs:element name="checksum" minOccurs="0">',
                f'{pad}  <xs:complexType>',
                f'{pad}    <xs:simpleContent>',
                f'{pad}      <xs:extension base="xs:string">',
                f'{pad}        <xs:attribute name="encoding" type="xs:string" use="required"/>',
                f'{pad}        <xs:attribute name="algorithm" type="xs:string" use="required"/>',
                f'{pad}      </xs:extension>',
                f'{pad}    </xs:simpleContent>',
                f'{pad}  </xs:complexType>',
                f'{pad}</xs:element>',
            ]

        ATTACHMENT_INNER_DEFAULT_ORDER = ["mimetype", "Size", "EASFile", "checksum", "FilePath"]

        def build_record_fields(props: dict, order: list[str], indent: int) -> list[str]:
            pad = " " * indent
            out: list[str] = []
            for name in order:
                prop = props.get(name, {})
                jtype = prop.get("type")
                fmt   = prop.get("format")

                # Force some fields to string (IDs, CCNs)
                if name in ALWAYS_STRING:
                    out.append(f'{pad}<xs:element name="{name}" type="xs:string"/>')
                    continue

                # Detect attachments container by inner "Attachment" object, use OUTER name from JSON
                is_attach_container = (
                    jtype == "object"
                    and isinstance(prop.get("properties"), dict)
                    and "Attachment" in prop["properties"]
                    and isinstance(prop["properties"]["Attachment"], dict)
                    and prop["properties"]["Attachment"].get("type") == "object"
                )
                if is_attach_container:
                    inner_obj = prop["properties"]["Attachment"]
                    inner_props = inner_obj.get("properties", {})
                    inner_order = inner_obj.get("xsd_sequence") or ATTACHMENT_INNER_DEFAULT_ORDER

                    out.append(f'{pad}<xs:element name="{name}">')  # OUTER name = CSV header
                    out.append(f'{pad}  <xs:complexType>')
                    out.append(f'{pad}    <xs:sequence>')
                    out.append(f'{pad}      <xs:element name="Attachment">')
                    out.append(f'{pad}        <xs:complexType>')
                    out.append(f'{pad}          <xs:sequence>')
                    for aname in inner_order:
                        if aname == "checksum":
                            out += build_checksum(indent + 12)
                        else:
                            a = inner_props.get(aname, {})
                            out.append(f'{pad}            <xs:element name="{aname}" type="{xs_type(a.get("type"), a.get("format"))}"/>')
                    out.append(f'{pad}          </xs:sequence>')
                    out.append(f'{pad}        </xs:complexType>')
                    out.append(f'{pad}      </xs:element>')
                    out.append(f'{pad}    </xs:sequence>')
                    out.append(f'{pad}  </xs:complexType>')
                    out.append(f'{pad}</xs:element>')
                    continue

                # Nested object (generic)
                if jtype == "object" and "properties" in prop:
                    inner_props = prop["properties"]
                    inner_order = prop.get("xsd_sequence") or list(inner_props.keys())
                    out.append(f'{pad}<xs:element name="{name}">')
                    out.append(f'{pad}  <xs:complexType>')
                    out.append(f'{pad}    <xs:sequence>')
                    out += build_record_fields(inner_props, inner_order, indent + 6)
                    out.append(f'{pad}    </xs:sequence>')
                    out.append(f'{pad}  </xs:complexType>')
                    out.append(f'{pad}</xs:element>')
                    continue

                # Plain scalar
                out.append(f'{pad}<xs:element name="{name}" type="{xs_type(jtype, fmt)}"/>')

            return out

        # Get Record + order
        record = (schema.get("properties") or {}).get(child_el, {})
        record_props = record.get("properties", {})
        record_order = record.get("xsd_sequence") or list(record_props.keys())

        target_ns = f"urn:eas-samples:en:xsd:{holding.lower()}.{XSD_VERSION}"
        lines = [
            f'<xs:schema elementFormDefault="qualified"',
            f'  targetNamespace="{target_ns}"',
            f'  version="{XSD_VERSION}" xmlns:xs="http://www.w3.org/2001/XMLSchema">',
            f'  <xs:element name="{root_el}">',
            f'    <xs:complexType>',
            f'      <xs:sequence maxOccurs="unbounded">',
            f'        <xs:element name="{child_el}" >',
            f'          <xs:complexType>',
            f'            <xs:sequence>',
        ]
        lines += build_record_fields(record_props, record_order, indent=14)
        lines += [
            f'            </xs:sequence>',
            f'          </xs:complexType>',
            f'        </xs:element>',
            f'      </xs:sequence>',
            f'    </xs:complexType>',
            f'  </xs:element>',
            f'</xs:schema>',
        ]
        return "\n".join(lines)

    @task()
    def write_xsd(xsd_str: str) -> str:
        with tracer.start_as_current_span("write_xsd"):
            ctx = get_current_context()
            holding = ctx["params"]["holding_name"].lower()
            os.makedirs(OUTPUT_FOLDER, exist_ok=True)
            path = os.path.join(OUTPUT_FOLDER, f"pdiSchema-urnEasSamplesEnXsd{holding}.{XSD_VERSION}.xsd")
            with open(path, "w", encoding="utf-8") as f:
                f.write(xsd_str)
            print(f"✅ XSD written to {path}")
            return path

    # Orchestration
    json_path = get_json_filename()
    schema = read_json(json_path)
    xsd_str = json_schema_to_xsd(schema)
    write_xsd(xsd_str)

convert_pdi_object_schema_to_xsd_dynamic_outer_dag = convert_pdi_object_schema_to_xsd_dynamic_outer_dag()
