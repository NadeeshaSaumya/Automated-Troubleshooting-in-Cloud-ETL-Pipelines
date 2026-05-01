import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
#from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import json
import csv
# import pandas as pd
from python_scripts.dag_status_check import get_failed_etl_from_rds
from python_scripts.snowflake import dataLoadMain
from python_scripts.snowflake import dataProcessingMain
from python_scripts.utils import utils
import pendulum
local_tz = pendulum.timezone("Asia/Colombo")
# -------------------------------------------------------------------
# CONFIG (Airflow Variables)
# -------------------------------------------------------------------

BUCKET = Variable.get("aiops_bucket_name").strip()
BASE_PREFIX = Variable.get("aiops_base_prefix")
OUTPUT_PREFIX = Variable.get("aiops_output_prefix")
CRITICAL_DAG_FILE = Variable.get("aiops_critical_dag_file")
NOC_ALERT_ROSTER_FILE = Variable.get("aiops_noc_alert_roster_file")

#SNOWFLAKE_CONN_ID = "snowflake_default"
env_name = Variable.get("env_name")
time_zone = Variable.get("time_zone")
sf_wh_name = Variable.get("sf_wh_name")
snowflake_secret_name = Variable.get("snowflake_secret_name")
rds_secret_name = Variable.get("rds_secret_name")
alert_sns_topic = Variable.get("alert_sns_topic")
local_tz = pendulum.timezone(time_zone)   

args = {
  'owner': 'Nadeesha_aiops',
  'description': 'ETL_Summary',
  #'depends_on_past' : True,
  'input_date' : datetime.now(local_tz).strftime("%Y-%m-%d"),
  'params': {
             'alert_sns_topic':alert_sns_topic,
             'alert_sns_region':'ap-southeast-1',
             'airflow_log':{ 
                        'process_name':'ETL_Summary',
                        "log_table":'airflow_logs',
                        'db_name':'etl_process'
             },
            'aws_secrets':{
                         "snowflake_secret":{
                                        "secret_name" : snowflake_secret_name,
                                        "region":"ap-southeast-1"
                                    },
                          "rds_secret":{
                                        "secret_name" : rds_secret_name,
                                        "region" : "ap-southeast-1"
                                    }
             },
             'control_table_validation' : {
                            'rds_control_table' : "etl_process.etl_exec_control_table",
                            'rds_control_db' : "etl_process",
                            'load_date_format' : '%Y%m%d',
                            'time_interval' : '1 DAY',
                            'loaddt_eq_sysdt' : 'yes'
             },
             'snowflake_load_config_dtls':{
                          "sf_wh_name" : sf_wh_name,
                          "alter_wh" : {
                                        "change_size_flag" : "NO",
                                        "new_size" : "XSMALL",
                                        "default_size" : "XSMALL"
                                    }
                         },
             'snowflake_dataprocessing_params':{
                          "sf_wh_name" : sf_wh_name,
                          "alter_wh" : {
                                        "change_size_flag" : "NO",
                                        "new_size" : "XSMALL",
                                        "default_size" : "XSMALL"
                                    }                          
                         }                           
            }
}


# -------------------------------------------------------------------
# BUSINESS LOGIC
# -------------------------------------------------------------------
def classify_default_reason(error_text: str) -> str:
    if not error_text:
        return "Other"

    text = error_text.lower()

    if "division by zero" in text:
        return "Source File Unavailability"

    if (
        "external sensor" in text
        or "sensor has timed out" in text
        or "sigterm" in text
    ):
        return "External Sensor Timeout"

    if "control_table_validation" in text:
        return "Control Table Failure"

    if "syntax_error" in text and "proc" in text:
        return "Deployment Issue"

    if "does not exist or not authorized" in text:
        return "Deployment Issue"

    if "glue_extraction_task" in text:
        return "Gluejob Error"

    return "Other"

def get_failure_type(dag_id):
    if dag_id and "kafka" in dag_id.lower():
        return "Kafka Related"
    return "ETL Failure"

def get_issue_type(default_reason):
    if default_reason == "Source File Unavailability":
        return "Source Issue"
    if default_reason == "Deployment Issue":
        return "Deployment Issue"
    else:
        return "Technical Issue"


def process_failed_dags(**context):

    s3 = boto3.client("s3")
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    rows = []
    prefix = f"{BASE_PREFIX}{date_str}/"

    print(f"Processing date: {date_str}")
    print(f"Bucket value: [{BUCKET}]")

    # ==========================
    # 1️⃣ Load Critical DAG List
    # ==========================
    response = s3.get_object(Bucket=BUCKET, Key=CRITICAL_DAG_FILE)
    content = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(content)

    critical_dags = {
        r["DAG Name"].strip().lower()
        for r in reader
        if r.get("DAG Name")
    }

    # ==========================
    # 2️⃣ Load SPOC from Monthly CSV
    # ==========================
    sheet_name = f"{str(yesterday.year)[-2:]}-{yesterday.strftime('%b')}"
    spoc_csv_key = f"AI-monitoring-project/config/output/{sheet_name}.csv"  # Adjust prefix if needed

    print(f"Loading SPOC CSV: {spoc_csv_key}")

    spoc = "UNKNOWN"

    try:
        response = s3.get_object(Bucket=BUCKET, Key=spoc_csv_key)
        content = response["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(content)

        for row in reader:
            try:
                row_date = datetime.strptime(row["Date"], "%Y-%m-%d").date()
                if row_date == yesterday.date():
                    spoc = row["1st contact 12.00A.M-11.59 P.M"].strip()
                    break
            except Exception:
                continue

    except Exception as e:
        print(f"Error loading SPOC CSV: {e}")

    print(f"SPOC for {yesterday.date()} is: {spoc}")

    # ==========================
    # 3️⃣ Process Failed DAG Logs
    # ==========================
    CSV_HEADERS = [
        "DAG_ID",
        "TASK_ID",
        "ISSUE_TYPE",
        "ENV",
        "DATE",
        "TIME",
        "PRIORITY",
        "DEFAULT_REASON",
        "FAILURE_TYPE",  
        "MODE_OF_ESCALATION",
        "STATUS",
        "TIME_TO_RESOLVE",
        "SPOC", 
        "ERROR",
        "RESOLUTION_STEPS",
        "REMARKS"
    ]
    rows = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("errors.json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
            data = json.loads(response["Body"].read())
            last_updated = data.get("last_updated")
            date_part, time_part = None, None
            if last_updated:
                date_part, time_part = last_updated.split(" ")
            dag_id = data.get("dag_id")

            error_text = " | ".join(
                log
                for block in data.get("error_blocks", [])
                for log in block.get("logs", [])
            )

            priority = (
                "CRITICAL"
                if dag_id and dag_id.lower() in critical_dags
                else "NON-CRITICAL"
            )
            default_reason = classify_default_reason(error_text)

            dag_id = data.get("dag_id")
            failure_type = get_failure_type(dag_id)
            issue_type = get_issue_type(default_reason)

 
            rows.append([
                data.get("dag_id"),
                data.get("task_id"),
                issue_type,  # ISSUE_TYPE
                data.get("airflow_environment"),
                date_part,
                time_part,
                priority,  # PRIORITY
                default_reason,
                failure_type,  # FAILURE_TYPE
                None,  # MODE_OF_ESCALATION
                None,  # STATUS
                None,   # TIME_TO_RESOLVE
                spoc,  # SPOC
                error_text,
                None,  # RESOLUTION_STEPS
                None,  # REASON
        ])


    # ==========================
    # 4️⃣ Save Output CSV
    # ==========================
    if not rows:
        print("No failed DAG logs found")
        return

    output_key = f"{OUTPUT_PREFIX}{date_str}/airflow_failed_dags.csv"
    local_csv = f"/tmp/airflow_failed_dags_{date_str}.csv"

    with open(local_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)

    s3.upload_file(local_csv, BUCKET, output_key)

    print(f"Uploaded output file to: {output_key}")

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------
DAG_NAME = 'aiops_airflow_etl_summary'

# args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

dag =  DAG(
    dag_id=DAG_NAME,
    default_args=args,
    start_date=datetime(2026,2,18,tzinfo=local_tz),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["aiops", "daily"])

init = EmptyOperator(
    task_id='start',
    dag=dag
)



process_task = PythonOperator(
    task_id="process_failed_dags",
    dag=dag,
    # provide_context=True,
    python_callable=process_failed_dags
)

rds_airflow_failed_logs = PythonOperator(
    task_id="rds_airflow_failed_logs", 
    dag=dag,
    provide_context=True,
    python_callable=get_failed_etl_from_rds.failed_etl_details_def,
    op_kwargs={ "process_id": 'DAILY_ETL_FAILURE_REPORTING_RDS', "args" : args},
    on_success_callback=utils.log_success_callback
   )

etl_summary_sf_loading_proc = PythonOperator(  #Chnaged variable name ,task id , process id and ,sp name - Parallel
    task_id="etl_summary_sf_loading_proc",
    dag=dag,
    # provide_context=True,
    python_callable=dataProcessingMain.callDataProcessingMain,
    op_kwargs={'args': args,'process_id': 'LOAD_AIRFLOW_FAILED_LOGS','sp_name': "PROD_DLK_DB.ANALYTICS_STG.LOAD_AIRFLOW_FAILED_LOGS" , 'constraint_list':{}},
    on_success_callback=utils.notify_success_email,
	on_failure_callback=utils.notify_email
	)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
    #on_success_callback=utils.notify_and_log_dag_success_callback
)

init >> process_task >>rds_airflow_failed_logs >> etl_summary_sf_loading_proc >> end_task