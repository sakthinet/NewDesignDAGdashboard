from airflow.decorators import dag, task
from datetime import datetime, timedelta
import subprocess, os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

OUTPUT_FOLDER = "/opt/airflow/data/output"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="iasip_ingest_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["infoarchive", "iashell", "ingest", "sip"],
    description="DAG to create IAShell script and execute ingestion via .bat file"
)




def iasip_ingest_dag(
    application_name: str = "CreditCardStatementsArchive",
    sipzips_folder: str = "/opt/airflow/data/sipzips/",
    iashell_path: str = "/mnt/iashell/bin/iashell"  # Update to Linux path or mount location
):
    @task()
    def get_valid_sip_zip(sipzips_folder: str):
        import zipfile
        valid_zips = []
        for fname in os.listdir(sipzips_folder):
            if fname.lower().endswith('.zip'):
                fpath = os.path.join(sipzips_folder, fname)
                try:
                    with zipfile.ZipFile(fpath, 'r') as zf:
                        bad_file = zf.testzip()
                        if bad_file is None:
                            valid_zips.append(fpath)
                except Exception as e:
                    print(f"❌ Corrupted zip skipped: {fpath} ({e})")
        if not valid_zips:
            raise FileNotFoundError(f"No valid SIP zip files found in {sipzips_folder}")
        # Optionally, pick the latest by modified time
        valid_zips.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        print(f"✅ Using SIP zip: {valid_zips[0]}")
        return valid_zips[0]

    @task()
    def create_iashell_script(application_name: str, sip_zip_path: str):
        env  = os.environ.copy()
        env["JAVA_HOME"] = "/usr/local/openjdk-11/bin/java"
        env["PATH"] = "/usr/local/openjdk-11/bin/java/bin:"+env.get("PATH","")
        # Use the folder path for --from, not a specific zip file
        script_content = f"connect\ningest applications/{application_name} --from {sip_zip_path}\n"
        script_filename = f"install-{application_name}.iashell"
        script_path = os.path.join(OUTPUT_FOLDER, script_filename)
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script_content)
        print(f"✅ IAShell script written to {script_path}")
        return script_path


    @task()
    def run_iashell(bat_file: str, iashell_script: str):
        # Call the Linux-compatible iashell executable directly
        # Ensure iashell_path is passed as a string, not a DagParam object
        env  = os.environ.copy()
        env["JAVA_HOME"] = "/usr/local/openjdk-11/bin/java"
        env["PATH"] = "/usr/local/openjdk-11/bin/java/bin:"+env.get("PATH","")
        if isinstance(bat_file, str):
            iashell_exec = bat_file
        else:
            iashell_exec = str(bat_file)
        command = f'"{iashell_exec}" script "{iashell_script}"'
        print(f"[DEBUG] Running command: {command}")
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            print("Output:\n", result.stdout)
            print("Errors:\n", result.stderr)
            return result.stdout
        except subprocess.CalledProcessError as e:
            print("Execution failed:\n", e.stderr)
            raise
        except Exception as e:
            print("Unexpected error:", e)
            raise

    # Use the folder path for ingestion, not a specific zip file
    iashell_script_path = create_iashell_script(application_name, sipzips_folder)
    # WARNING: The following task will fail in Linux environments because .bat files and 'cmd' are Windows-only.
    # To run ingestion in Linux, use a compatible shell script or run Airflow on Windows.
    run_iashell(iashell_path, iashell_script_path)


iasip_ingest_dag = iasip_ingest_dag()
