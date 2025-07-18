from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import psycopg2
from contextlib import closing
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import NearestNeighbors


# PostgreSQL connection config
POSTGRES_CONN_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5432,
    'dbname': 'RND',
    'user': 'postgres',
    'password': 'postgres'
}

REPORTS_DIR = '/opt/airflow/reports'
os.makedirs(REPORTS_DIR, exist_ok=True)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'retries': 1
}

def get_postgres_conn():
    return psycopg2.connect(**POSTGRES_CONN_CONFIG)

with DAG(
    dag_id='MetadataAnalysis',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["BI on Document Metadata"]
) as dag:

    # ──────── EXTRACTION TASKS ────────

    @task
    def extract_document_types():
        sql = "SELECT document_type, COUNT(*) as count FROM documents GROUP BY document_type ORDER BY count DESC"
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.to_dict(orient='records')
    
    @task
    def extract_document_status():
        sql = """
            SELECT document_status, COUNT(*) as count
            FROM documents
            GROUP BY document_status
            ORDER BY count DESC
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.to_dict(orient='records')

    @task
    def extract_sensitivity_labels():
        sql = """
            SELECT sensitivity_labels, COUNT(*) as count
            FROM documents
            GROUP BY sensitivity_labels
            ORDER BY count DESC
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.to_dict(orient='records')

    @task
    def extract_retention_policies():
        sql = """
            SELECT retention_policies, COUNT(*) as count
            FROM documents
            GROUP BY retention_policies
            ORDER BY count DESC
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.to_dict(orient='records')


    @task
    def extract_document_age():
        sql = """
        SELECT 
            FLOOR(EXTRACT(DAY FROM NOW() - creation_date)/365) AS age_years,
            COUNT(*) as count
        FROM documents
        GROUP BY age_years
        ORDER BY age_years
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        age_bins = pd.DataFrame({'age_years': range(0, 6)})
        df = age_bins.merge(df, how='left').fillna(0)
        return df.to_dict(orient='records')

    @task
    def extract_monthly_volume():
        sql = """
                WITH monthly_counts AS (
        SELECT 
            DATE_TRUNC('month', creation_date)::DATE AS month,
            COUNT(*) AS new_docs
        FROM documents
        GROUP BY DATE_TRUNC('month', creation_date)::DATE
        )
        SELECT 
            month,
            new_docs,
            SUM(new_docs) OVER (ORDER BY month) AS cumulative_total
        FROM monthly_counts
        ORDER BY month
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
            df['month'] = pd.to_datetime(df['month'])  # ✨ the fix
            df['month'] = df['month'].dt.strftime('%Y-%m')
        return df.to_dict(orient='records')


    @task
    def extract_metadata_completeness():
        sql = """
        SELECT 
            ROUND(100.0 * COUNT(*) FILTER (WHERE document_type IS NULL) / COUNT(*), 2) AS missing_type,
            ROUND(100.0 * COUNT(*) FILTER (WHERE creator IS NULL) / COUNT(*), 2) AS missing_creator,
            ROUND(100.0 * COUNT(*) FILTER (WHERE retention_policies IS NULL) / COUNT(*), 2) AS missing_retention
        FROM documents
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.iloc[0].to_dict()

    @task
    def extract_classification():
        sql = "SELECT sensitivity_labels as classification, COUNT(*) as count FROM documents GROUP BY classification"
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.to_dict(orient='records')

    @task
    def extract_duplicates():
        sql = """
        WITH duplicates AS (
            SELECT folder_path, filename, COUNT(*) as occurrences
            FROM documents
            GROUP BY folder_path, filename
            HAVING COUNT(*) > 1
        )
        SELECT 
            COUNT(*) as duplicate_files,
            SUM(occurrences) as total_occurrences
        FROM duplicates
        """
        with closing(get_postgres_conn()) as conn:
            df = pd.read_sql(sql, con=conn)
        return df.iloc[0].to_dict()

    # ──────── PROCESSING TASKS ────────

    @task
    def process_document_status(raw):
        total = sum(item['count'] for item in raw)
        result = {
            "labels": [item['document_status'] for item in raw],
            "counts": [item['count'] for item in raw],
            "percentages": [round(item['count'] / total * 100, 2) for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "document_status_stats.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path


    @task
    def process_sensitivity_labels(raw):
        total = sum(item['count'] for item in raw)
        result = {
            "labels": [item['sensitivity_labels'] for item in raw],
            "counts": [item['count'] for item in raw],
            "percentages": [round(item['count'] / total * 100, 2) for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "sensitivity_labels_stats.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path
    
    @task
    def process_retention_policies(raw):
        total = sum(item['count'] for item in raw)
        result = {
            "labels": [item['retention_policies'] for item in raw],
            "counts": [item['count'] for item in raw],
            "percentages": [round(item['count'] / total * 100, 2) for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "retention_policies_stats.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path


    @task
    def process_document_types(raw):
        total = sum(item['count'] for item in raw)
        result = {
            "labels": [item['document_type'] for item in raw],
            "counts": [item['count'] for item in raw],
            "percentages": [round(item['count'] / total * 100, 2) for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "document_type_stats.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path

    @task
    def process_document_age(raw):
        result = {
            "age_bins": [f"{int(item['age_years'])}-{int(item['age_years'])+1}" for item in raw],
            "counts": [item['count'] for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "document_age_distribution.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path

    @task
    def process_monthly_volume(raw):
        path = os.path.join(REPORTS_DIR, "monthly_growth.json")
        with open(path, 'w') as f:
            json.dump(raw, f)
        return path

    @task
    def process_metadata_completeness(raw):
        result = {
            "document_type": 100 - raw['missing_type'],
            "creator": 100 - raw['missing_creator'],
            "retention_policy": 100 - raw['missing_retention']
        }
        path = os.path.join(REPORTS_DIR, "metadata_completeness.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path

    @task
    def process_classification(raw):
        total = sum(item['count'] for item in raw)
        result = {
            "labels": [item['classification'] for item in raw],
            "counts": [item['count'] for item in raw],
            "percentages": [round(item['count'] / total * 100, 2) for item in raw]
        }
        path = os.path.join(REPORTS_DIR, "classification_distribution.json")
        with open(path, 'w') as f:
            json.dump(result, f)
        return path

    @task
    def process_duplicates(raw):
        path = os.path.join(REPORTS_DIR, "duplicate_analysis.json")
        with open(path, 'w') as f:
            json.dump(raw, f)
        return path
    
#    @task
#    def load_metadata():
#        sql = "SELECT * FROM documents"
#        with closing(get_postgres_conn()) as conn:
#            df = pd.read_sql(sql, con=conn)
#        return df

    @task
    def load_metadata():
        engine = create_engine("postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/RND")
        try:
            sql = "SELECT * FROM documents"
            df = pd.read_sql(sql, con=engine)
            output_path = "/tmp/reports/documents.csv"

            if df.empty:
                print("⚠️ Loaded metadata is empty.")
            else:
                print(f"✅ Loaded {len(df)} records.")
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                df.to_csv(output_path, index=False)

            return output_path  # returning the CSV path
        except Exception as e:
            print(f"🔥 load_metadata() failed: {str(e)}")
            raise


    @task
    def detect_document_outliers(csv_path: str):
        df = pd.read_csv(csv_path)
        df_features = pd.get_dummies(
            df[["version", "document_status", "retention_policies", "sensitivity_labels"]].fillna("Unknown")
        )
        model = IsolationForest(contamination=0.05, random_state=42)
        df["outlier_score"] = model.fit_predict(df_features)

        path = os.path.join(REPORTS_DIR, "document_outliers.json")
        df[df["outlier_score"] == -1][["filename", "creator", "last_modified"]].to_json(path, orient="records", indent=2)
        return path


    @task
    def recommend_sensitivity_labels(csv_path: str):
        df = pd.read_csv(csv_path)
        df_labeled = df.dropna(subset=["sensitivity_labels", "modifier"])

        if df_labeled.empty:
            print("⚠️ No labeled documents found with valid modifiers.")
            return None

        user_enc = pd.get_dummies(df_labeled["modifier"])
        knn = NearestNeighbors(n_neighbors=3, metric="cosine").fit(user_enc)

        recommendations = {}
        candidates = df[df["sensitivity_labels"].isna() & df["modifier"].notna()]
        for _, row in candidates.iterrows():
            user_vec = pd.get_dummies(pd.Series([row["modifier"]])).reindex(columns=user_enc.columns, fill_value=0)
            dists, idxs = knn.kneighbors(user_vec)
            neighbor_labels = df_labeled.iloc[idxs[0]]["sensitivity_labels"]
            suggestions = neighbor_labels.mode()
            if not suggestions.empty:
                recommendations[row["filename"]] = suggestions[0]

        path = os.path.join(REPORTS_DIR, "sensitivity_label_suggestions.json")
        with open(path, 'w') as f:
            json.dump(recommendations, f, indent=2)
        return path



    # ──────── FINAL SUMMARY REPORT ────────

    @task
    def generate_summary_report(outputs):
        summary = {
            "timestamp": datetime.now().isoformat(),
            "analyses": {
                "document_types": outputs[0],
                "document_age": outputs[1],
                "monthly_growth": outputs[2],
                "metadata_completeness": outputs[3],
                "classification": outputs[4],
                "duplicate_analysis": outputs[5],
                "document_status": outputs[6],
                "sensitivity_labels": outputs[7],
                "retention_policies": outputs[8],
                "outlier_documents": outputs[9],
                "sensitivity_label_recommendations": outputs[10]
            }
        }
        path = os.path.join(REPORTS_DIR, "summary_report.json")
        with open(path, 'w') as f:
            json.dump(summary, f)
        return path



    # ──────── DAG DEPENDENCIES ────────

    doc_types = process_document_types(extract_document_types())
    doc_statuses = process_document_status(extract_document_status())
    sensitivities = process_sensitivity_labels(extract_sensitivity_labels())
    retention = process_retention_policies(extract_retention_policies())
    doc_age = process_document_age(extract_document_age())
    volume = process_monthly_volume(extract_monthly_volume())
    meta = process_metadata_completeness(extract_metadata_completeness())
    classify = process_classification(extract_classification())
    dupes = process_duplicates(extract_duplicates())
    csv_path = load_metadata()
    outliers = detect_document_outliers(csv_path)
    label_recommendations = recommend_sensitivity_labels(csv_path)  

    final_outputs = [
        doc_types,          # 0
        doc_age,            # 1
        volume,             # 2
        meta,               # 3
        classify,           # 4
        dupes,              # 5
        doc_statuses,       # 6
        sensitivities,      # 7
        retention,          # 8
        outliers,           # 9 NEW
        label_recommendations  # 10 NEW
    ]

    generate_summary_report(final_outputs)

