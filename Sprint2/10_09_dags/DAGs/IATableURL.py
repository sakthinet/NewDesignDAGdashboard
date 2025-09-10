from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

# Configurable Inputs
IA_HOST = "http://10.73.91.23:8765"
BEARER_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdWVAaWFjdXN0b21lci5jb20iLCJ1c2VyX25hbWUiOiJzdWVAaWFjdXN0b21lci5jb20iLCJpc3MiOiJodHRwOi8vMTAuNzMuOTEuMjM6ODA4MCIsInRva2VuX3R5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiJpbmZvYXJjaGl2ZS5jbGkiLCJhdXRob3JpdGllcyI6WyJHUk9VUF9SRVRFTlRJT05fTUFOQUdFUiIsIkdST1VQX0RFVkVMT1BFUiIsIkdST1VQX0lUX09XTkVSIiwiR1JPVVBfQlVTSU5FU1NfT1dORVIiLCJHUk9VUF9BRE1JTklTVFJBVE9SIiwiR1JPVVBfRU5EX1VTRVIiLCJST0xFX0FETUlOSVNUUkFUT1IiLCJST0xFX0JVU0lORVNTX09XTkVSIiwiUk9MRV9ERVZFTE9QRVIiLCJST0xFX0VORF9VU0VSIiwiUk9MRV9JVF9PV05FUiIsIlJPTEVfUkVURU5USU9OX01BTkFHRVIiXSwiYXVkIjoiaW5mb2FyY2hpdmUuY2xpIiwibmJmIjoxNzUxODcwMTM4LCJncmFudF90eXBlIjoicGFzc3dvcmQiLCJzY29wZSI6WyJzZWFyY2giXSwiZXhwIjozODk5MzUzNzg1LCJpYXQiOjE3NTE4NzAxMzgsImp0aSI6IjQzZjE3MzM3LTdiZWItNGJlOC04NWMzLTZhYzFjMWEzZTA4ZCJ9.OEOX1DoN_d-YHKDBDf290bYRrr2IiujjQqkolLSh34M"

default_args = {
    'owner': 'Harish',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="infoarchive_table_url_lookupupdated",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["infoarchive", "metadata", "lookup"]
)
def infoarchive_table_url_dag(
    APPLICATION_NAME: str = "SAP-PO",
    SCHEMA_NAME: str = "SAP",
    TABLE_NAME: str = "PurchaseOrderHeader"
):
    @task()
    def get_tenant_id(token: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/hal+json"}
        tenants = requests.get(f"{IA_HOST}/systemdata/tenants", headers=headers).json()
        return tenants["_embedded"]["tenants"][0]["id"]

    @task()
    def get_application_id(token: str, tenant_id: str, application_name: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/hal+json"}
        apps_url = f"{IA_HOST}/systemdata/tenants/{tenant_id}/applications"
        apps = requests.get(apps_url, headers=headers).json()
        for app in apps["_embedded"]["applications"]:
            if app["name"] == application_name:
                return app["id"]
        raise ValueError("Application not found")

    @task()
    def get_database_id(token: str, app_id: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/hal+json"}
        db_url = f"{IA_HOST}/systemdata/applications/{app_id}/databases"
        dbs = requests.get(db_url, headers=headers).json()
        return dbs["_embedded"]["databases"][0]["id"]

    @task()
    def get_schema_id(token: str, db_id: str, schema_name: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/hal+json"}
        schema_url = f"{IA_HOST}/systemdata/databases/{db_id}/schemas"
        schemas = requests.get(schema_url, headers=headers).json()
        for schema in schemas["_embedded"]["schemas"]:
            if schema["name"] == schema_name:
                return schema["id"]
        raise ValueError("Schema not found")

    @task()
    def get_table_url(token: str, schema_id: str, table_name: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/hal+json"}
        table_url = f"{IA_HOST}/systemdata/schemas/{schema_id}/tables"
        tables = requests.get(table_url, headers=headers).json()
        for table in tables["_embedded"]["tables"]:
            if table["name"] == table_name:
                return table["_links"]["self"]["href"]
        raise ValueError("Table not found")

    @task()
    def log_result(table_url: str):
        print(f"✅ Final Table URL: {table_url}")

    # DAG orchestration
    token = BEARER_TOKEN
    tenant_id = get_tenant_id(token)
    app_id = get_application_id(token, tenant_id, APPLICATION_NAME)
    db_id = get_database_id(token, app_id)
    schema_id = get_schema_id(token, db_id, SCHEMA_NAME)
    table_url = get_table_url(token, schema_id, TABLE_NAME)
    log_result(table_url)


dag_instance = infoarchive_table_url_dag()