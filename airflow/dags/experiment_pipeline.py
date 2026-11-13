from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime
API="http://api:8080"
def consume_kafka():
    print("[Apache Kafka] Consuming events.raw, checkpointing offsets...")
def aggregate():
    print("[Airflow] Aggregating by variant: control vs treatment")
def build_scorecard():
    try:
        r=requests.get(f"{API}/api/experiments", timeout=5)
        exps=r.json()
        if exps:
            last=exps[-1]
            name=last.get("Name") or last.get("name") or "checkout-button-test"
            r2=requests.post(f"{API}/api/scorecards/compute?experiment={name}", timeout=5)
            print(f"[Airflow] Scorecard built for {name}: {r2.text[:500]}")
        else:
            print("[Airflow] No experiments yet, skipping scorecard - Did you click Publish in Blazor? Designer alone does NOT save.")
    except Exception as e:
        print(f"[Airflow] Scorecard failed (API not ready?): {e}")
with DAG("experiment_pipeline", start_date=datetime(2026,1,1), schedule="@hourly", catchup=False, tags=["ic3","experimentation"]) as dag:
    t1=PythonOperator(task_id="consume_kafka_events_raw", python_callable=consume_kafka)
    t2=PythonOperator(task_id="aggregate_by_variant", python_callable=aggregate)
    t3=PythonOperator(task_id="build_scorecard", python_callable=build_scorecard)
    t1>>t2>>t3
