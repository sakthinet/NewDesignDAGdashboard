import os
import requests
import json
import uuid
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import re
from tempfile import mkdtemp
from airflow.exceptions import AirflowSkipException
from typing import Literal
import hashlib
from typing import List, Dict
from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from qdrant_client.http.models import (
    VectorParams,
    Distance,
    ScalarQuantization,
    ScalarQuantizationConfig,
    ScalarType
)

# CONFIG
INPUT_FOLDER = Variable.get("idp_input_folder", "/opt/airflow/data/input_docs")
DOCS_PER_RUN = int(Variable.get("docs_per_run", 10))  # Number of docs to ingest per run
CHUNK_STRATEGY = Variable.get("chunk_strategy", "heading")  # fixed, sentence, heading, semantic, agentic, or hybrid.
VLM_API_URL = "http://host.docker.internal:11434/api/generate"  # Bakllava VLM OCR endpoint
EMBEDDINGS_API_URL = "http://host.docker.internal:11434/api/embeddings"  # Ollama embeddings endpoint
QDRANT_URL = "http://host.docker.internal:6333"  # Qdrant REST API endpoint
QDRANT_HOST = "http://host.docker.internal"
QDRANT_PORT = "6333"
QDRANT_COLLECTION = "document_chunks"
MODEL_NAME = "nomic-embed-text"
VECTOR_SIZE = 1024
os.environ["OLLAMA_BASE_URL"] = "http://host.docker.internal:11434/v1"


with DAG(
    dag_id="DataForGenAI",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["IDP", "Vectorization"],
) as dag:

    @task
    def import_documents(folder: str, limit: int) -> list[str]:
        files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if os.path.isfile(os.path.join(folder, f))
        ]
        return files[:limit]

    @task
    def classify_file_type(file_path: str) -> str:
        # Returns 'image', 'pdf', or 'text' using PyMuPDF
        import fitz  # PyMuPDF
        try:
            doc = fitz.open(file_path)
            if doc.is_pdf:
                for page in doc:
                    text = page.get_text().strip()
                    if text:  # Found actual text
                        return 'pdf_text'
                return 'pdf_image'  # No text found on any page
            elif doc.is_image:
                return 'image'
        except Exception:
            pass
        return 'text'
    
    @task
    def extract_document_text(file_path: str, file_type: str) -> str:
        if file_type == 'pdf_image': 
                extracted_text = []
                pages=[]
                temp_dir=''
                # Process each page with OCR
                for i, page in enumerate(pages):
                    image_path = os.path.join(temp_dir, f"page_{i+1}.png")
                    page.save(image_path, "PNG")
                    #ocr = OCRProcessor(model_name='llama3.2-vision:11b')
                    ocr=""
                    result = ocr.process_image(
                        image_path=image_path,
                        format_type="text",
                        custom_prompt="Extract all visible text with correct formatting and layout",
                        language="English"
                    )  
                    extracted_text.append(result)
                    print(f"🧠 OCR Text Extracted from Page {i+1}")
                    
                # Combine all pages' text
                full_text = "\n\n".join(extracted_text)
        elif file_type == 'pdf_text':
            import fitz
            doc = fitz.open(file_path)
            text = ""
            for page in doc:
                text += page.get_text()
            return text.strip()
        else:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()

    #
    # chunk_text: Splits a document's text into smaller, manageable pieces ("chunks") for downstream processing.
    # The chunking method is configurable via the 'strategy' argument:
    #   - fixed:     Chunks of N words (default 500)
    #   - sentence:  Chunks of N sentences (default 5, uses NLTK)
    #   - heading:   Splits at headings (Markdown, all-caps, numbered)
    #   - semantic:  Topic modeling (LDA) to group paragraphs by topic
    #   - agentic:   Uses locally hosted Deepseek LLM to decide chunk boundaries
    #   - hybrid:    Combines heading and fixed-size chunking
    #   - default:   Paragraph-based (double newlines)
    #
    @task
    def chunk_text(text: str, strategy: str) -> list[str]:
        import re
        if strategy == "fixed":
            # Fixed size chunking (e.g., 500 words)
            words = text.split()
            chunk_size = 500
            return [" ".join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)]
        elif strategy == "sentence":
            # Sentence-based chunking using regex (e.g., 5 sentences per chunk)
            # Splits on punctuation followed by whitespace and capital letter
            sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text.strip())
            chunk_size = 5
            return [" ".join(sentences[i:i+chunk_size]) for i in range(0, len(sentences), chunk_size)]
        elif strategy == "heading":
            # Header based chunking (split on headings)
            chunks = re.split(r"(?:^|\n)(?:#+\s|[A-Z][A-Z\s]+:|\d+\.\s)", text)
            return [c.strip() for c in chunks if c.strip()]
        elif strategy == "semantic":
            # Semantic chunking using topic modeling (simple LDA example)
            from sklearn.feature_extraction.text import CountVectorizer
            from sklearn.decomposition import LatentDirichletAllocation
            paras = [p for p in text.split("\n\n") if p.strip()]
            if len(paras) < 2:
                return paras
            vectorizer = CountVectorizer(stop_words='english')
            X = vectorizer.fit_transform(paras)
            lda = LatentDirichletAllocation(n_components=min(3, len(paras)), random_state=42)
            topics = lda.fit_transform(X)
            topic_assignments = topics.argmax(axis=1)
            chunks = {}
            for idx, topic in enumerate(topic_assignments):
                chunks.setdefault(topic, []).append(paras[idx])
            return ["\n\n".join(chunk) for chunk in chunks.values()]
        elif strategy == "agentic":
            # Agentic chunking using locally hosted Deepseek LLM
            OLLAMA_LLM_URL = "http://host.docker.internal:11434/api/generate"
            prompt = (
                "You are an expert document chunker. Split the following text into logical, meaningful sections. "
                "Return a JSON list of strings, each string is a chunk.\nTEXT:\n" + text[:8000]
            )
            r = requests.post(OLLAMA_LLM_URL, json={"model": "deepseek-coder:latest", "prompt": prompt})
            r.raise_for_status()
            import json as _json
            try:
                return _json.loads(r.json()["choices"][0]["text"])
            except Exception:
                return [text]
        elif strategy == "hybrid":
            # Hybrid: combine heading and fixed size
            # First split by heading, then further split large chunks by fixed size
            heading_chunks = re.split(r"(?:^|\n)(?:#+\s|[A-Z][A-Z\s]+:|\d+\.\s)", text)
            final_chunks = []
            chunk_size = 500
            for chunk in heading_chunks:
                words = chunk.split()
                if len(words) > chunk_size:
                    final_chunks.extend([" ".join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)])
                elif chunk.strip():
                    final_chunks.append(chunk.strip())
            return final_chunks
        else:
            # Default: paragraph split
            return [chunk.strip() for chunk in text.split("\n\n") if chunk.strip()]

    @task
    def vectorize_chunks(chunks: list[str]) -> list[dict]:
        vectors = []
        print(f"📦 Number of chunks received: {len(chunks)}")
        for chunk in chunks:

            response = requests.post(
                EMBEDDINGS_API_URL ,
                json={
                    "model": MODEL_NAME,
                    "prompt": chunk,
                    "options": {"embedding_only": True}
                }
            )
            response.raise_for_status()
            data = response.json()
            embedding = data.get("embedding")
            if embedding:
                vectors.append({"text": chunk, "vector": embedding})
            else:
                raise ValueError(f"Invalid embedding received for chunk: {chunk}")
        return vectors

    def ensure_optimized_qdrant_collection(client, collection_name: str, vector_size: int):
        existing_collections = [col.name for col in client.get_collections().collections]

        if collection_name not in existing_collections:
            client.recreate_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE,
                    on_disk=True
                ),
                quantization_config=ScalarQuantization(
                    scalar=ScalarQuantizationConfig(
                        type=ScalarType.INT8,
                        always_ram=True
                    )
                )
            )

    @task
    def export_to_qdrant(vectors: list[dict], file_path: str):
        filename = os.path.basename(file_path)
        vectorsize=len(vectors[0]["vector"])
        points = []
        for i, v in enumerate(vectors):
            points.append(PointStruct(
                id= str(uuid.uuid4()),
                vector=v["vector"],
                payload={
                    "text": v["text"],
                    "source_file": filename
                }
            ))
        client = QdrantClient(url=QDRANT_URL)
        ensure_optimized_qdrant_collection(
                client=client,
                collection_name="document_chunks",  # your desired collection name
                vector_size=vectorsize                     # set this to match your embeddings' dimensionality
                )
        client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=points
        )
        return True
       
       

    files = import_documents(INPUT_FOLDER, DOCS_PER_RUN)
    file_type = classify_file_type.expand(file_path=files)
    text = extract_document_text.expand(file_path=files, file_type=file_type)
    chunks = chunk_text.partial(strategy=CHUNK_STRATEGY).expand(text=text)
    vectors = vectorize_chunks.expand(chunks=chunks)
    export = export_to_qdrant.expand(vectors=vectors, file_path=files)

    files >> file_type >> text >> chunks >> vectors >> export
