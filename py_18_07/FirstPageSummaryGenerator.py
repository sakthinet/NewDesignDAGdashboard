from airflow import DAG
from airflow.decorators import task
import os, re, requests, psycopg2

FOLDER_PATH = "/opt/airflow/data"
MAX_FILES = 10

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="FirstPageSummaryGenerator",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Modular file processor using DeepSeek LLM and PostgreSQL",
    max_active_runs=1,
     tags=["Abstractive Summarization"]
) as dag:

    @task()
    def scan_folder() -> list[dict]:
        return [
            {"filename": f}
            for f in os.listdir(FOLDER_PATH)
            if f.endswith(".txt") and "_Processed" not in f
        ][:MAX_FILES]

    @task()
    def extract_first_page(file_meta: dict) -> dict:
        filename = file_meta["filename"]
        path = os.path.join(FOLDER_PATH, filename)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = "".join([f.readline() for _ in range(50)])
        return {"filename": filename, "text": text}

    @task(max_active_tis_per_dag=1)
    def summarize_file(file_data: dict) -> dict:
        prompt = f"Provide a concise abstractive summary of the following document in 5 sentences:\n\n{file_data['text']}"
        response = requests.post(
            "http://host.docker.internal:11434/api/generate",
            headers={"Content-Type": "application/json"},
            json={"model": "deepseek-r1:8b", "prompt": prompt, "stream": False}
        )
        raw_summary = response.json().get("response", "")
        summary = re.sub(r"<think>.*?</think>", "", raw_summary, flags=re.DOTALL).strip()
        return {"filename": file_data["filename"], "summary": summary}

    @task()
    def insert_to_db(record: dict) -> None:
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="RND",
            user="postgres",
            password="postgres",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS summaries (
                id SERIAL PRIMARY KEY,
                filename TEXT,
                summary TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cursor.execute(
            "INSERT INTO summaries (filename, summary) VALUES (%s, %s);",
            (record["filename"], record["summary"])
        )
        conn.commit()
        cursor.close()
        conn.close()

    @task()
    def rename_file(file_meta: dict) -> None:
        filename = file_meta["filename"]
        src = os.path.join(FOLDER_PATH, filename)
        dst = os.path.join(FOLDER_PATH, filename.replace(".txt", "_Processed.txt"))
        os.rename(src, dst)

    # DAG wiring
    files_meta = scan_folder()
    page_data = extract_first_page.expand(file_meta=files_meta)
    summaries = summarize_file.expand(file_data=page_data)
    insertions = insert_to_db.expand(record=summaries)
    renames = rename_file.expand(file_meta=files_meta)

    files_meta >> page_data >> summaries >> insertions >> renames