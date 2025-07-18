from airflow import DAG
from airflow.decorators import task
import os, re, requests, psycopg2

FOLDER_PATH = "/opt/airflow/data"
MAX_FILES = 10
DEEPSEEK_URL = "http://host.docker.internal:11434/api/generate"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

def query_deepseek(prompt: str) -> str:
    response = requests.post(
        DEEPSEEK_URL,
        headers={"Content-Type": "application/json"},
        json={"model": "deepseek-r1:8b", "prompt": prompt, "stream": False}
    )
    if not response.ok:
        return ""
    return re.sub(r"<think>.*?</think>", "", response.json().get("response", ""), flags=re.DOTALL).strip()

with DAG(
    dag_id="ContentEnrichment",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["Deepseek LLM", "Transformation", "Enrichment"],
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

    @task()
    def summarize_file(doc: dict) -> dict:
        prompt = f"Create a concise abstractive summary of the following text in 5 sentences\n\n{doc['text']}"
        return {**doc, "summary": query_deepseek(prompt)}

    @task()
    def extract_keywords(doc: dict) -> dict:
        prompt = f"Extract the top 10 keywords or phrases:\n\n{doc['text']}"
        return {**doc, "keywords": query_deepseek(prompt)}

    @task()
    def classify_topic(doc: dict) -> dict:
        prompt = f"What is the primary topic or category of this text in 1-5 words?\n\n{doc['text']}"
        return {**doc, "topic": query_deepseek(prompt)}

    @task()
    def extract_named_entities(doc: dict) -> dict:
        prompt = f"List all named entities (people, organizations, locations, dates):\n\n{doc['text']}"
        return {**doc, "named_entities": query_deepseek(prompt)}

    @task()
    def detect_pii(doc: dict) -> dict:
        prompt = f"Identify all personally identifiable information in this text:\n\n{doc['text']}"
        return {**doc, "pii_entities": query_deepseek(prompt)}

    @task()
    def detect_intent(doc: dict) -> dict:
        prompt = f"What is the intent or purpose of this text in 1-5 words?\n\n{doc['text']}"
        return {**doc, "intent": query_deepseek(prompt)}

    @task()
    def expand_synonyms(doc: dict) -> dict:
        prompt = f"List synonyms or alternate terms for key ideas in this text:\n\n{doc['text']}"
        return {**doc, "synonyms": query_deepseek(prompt)}

    @task()
    def insert_to_db(record: dict) -> dict:
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="RND",
            user="postgres",
            password="postgres",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformations (
                id SERIAL PRIMARY KEY,
                filename TEXT,
                summary TEXT,
                keywords TEXT,
                topic TEXT,
                named_entities TEXT,
                pii_entities TEXT,
                intent TEXT,
                synonyms TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cursor.execute("""
            INSERT INTO transformations (
                filename, summary, keywords, topic,
                named_entities, pii_entities, intent, synonyms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            record["filename"],
            record.get("summary", ""),
            record.get("keywords", ""),
            record.get("topic", ""),
            record.get("named_entities", ""),
            record.get("pii_entities", ""),
            record.get("intent", ""),
            record.get("synonyms", "")
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return {"filename": record["filename"]}

    @task()
    def rename_file(file_meta: dict) -> None:
        filename = file_meta["filename"]
        src = os.path.join(FOLDER_PATH, filename)
        dst = os.path.join(FOLDER_PATH, filename.replace(".txt", "_Processed.txt"))
        os.rename(src, dst)

    # Arrow-style mapping wiring
    files_meta = scan_folder()
    page_data = extract_first_page.expand(file_meta=files_meta)
    summaries = summarize_file.expand(doc=page_data)
    keywords = extract_keywords.expand(doc=summaries)
    topics = classify_topic.expand(doc=keywords)
    entities = extract_named_entities.expand(doc=topics)
    pii = detect_pii.expand(doc=entities)
    intents = detect_intent.expand(doc=pii)
    synonyms = expand_synonyms.expand(doc=intents)
    insertions = insert_to_db.expand(record=synonyms)
    renames = rename_file.expand(file_meta=files_meta)

    # Dependency chain
    files_meta >> page_data >> summaries >> keywords >> topics >> entities >> pii >> intents >> synonyms >> insertions >> renames
