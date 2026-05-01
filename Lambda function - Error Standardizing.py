import os
import re
import json
import gzip
import base64
import boto3
import logging
from datetime import datetime, timedelta, timezone

# === Configure logging ===
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === AWS Clients ===
s3_client = boto3.client("s3")

# === Environment variables ===
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
MWAA_ENVIRONMENT = os.environ.get("MWAA_ENVIRONMENT")
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

# === Regex patterns ===
FAILURE_PATTERN = re.compile(r"(?i)\b(error|exception|task failed|failed)\b")
HEADER_PATTERN = re.compile(r"\[([^\]]+)\]\s+\{([^\:]+):(\d+)\}\s+(.*)")

# ✅ Excluded error pattern
EXCLUDED_PATTERNS = [
    "Error when executing notify_success_email callback"
]

# === Helper to extract DAG metadata from logStream ===
def extract_airflow_metadata(log_stream):
    """
    Extract dag_id, run_id, task_id, attempt from the CloudWatch logStream name.
    Example:
    dag_id=DTV_SACHET_ACTIVATION_LOG_VIEW_ETL_PROCESS_DAILY/run_id=scheduled__2025-11-05T01_30_00+00_00/task_id=notify_success_email/attempt=1
    """
    dag_match = re.search(r"dag_id=([^/]+)", log_stream)
    run_match = re.search(r"run_id=([^/]+)", log_stream)
    task_match = re.search(r"task_id=([^/]+)", log_stream)
    attempt_match = re.search(r"attempt=(\d+)", log_stream)

    return {
        "dag_id": dag_match.group(1) if dag_match else "UNKNOWN_DAG",
        "run_id": run_match.group(1) if run_match else "UNKNOWN_RUN",
        "task_id": task_match.group(1) if task_match else "UNKNOWN_TASK",
        "attempt": int(attempt_match.group(1)) if attempt_match else None,
    }


def extract_failed_logs(messages):
    """
    Extract multi-line failed log blocks from decoded messages.
    Groups consecutive lines that belong to the same error or traceback.
    Handles errors from taskinstance.py, standard_task_runner.py, and others.
    """
    log_blocks = []
    current_block = []
    current_meta = {"timestamp": None, "task": None}

    for line in messages:
        header_match = HEADER_PATTERN.match(line)
        if header_match:
            timestamp, task_name, task_num, log_line = header_match.groups()
            current_task = f"{task_name}:{task_num}"

            # ✅ Skip unwanted patterns
            if any(skip_text in log_line for skip_text in EXCLUDED_PATTERNS):
                continue

            # ✅ Start a new error block if "error"/"failed"/"exception" appears
            if FAILURE_PATTERN.search(log_line):
                if current_block:
                    log_blocks.append({
                        "timestamp": current_meta["timestamp"],
                        "task": current_meta["task"],
                        "logs": current_block
                    })
                current_block = [log_line.strip()]
                current_meta = {"timestamp": timestamp, "task": current_task}

            # ✅ If already in a block, append all following lines (continuation)
            elif current_block:
                current_block.append(log_line.strip())

        else:
            # ✅ Continuation line outside a header — maybe traceback or continuation
            if current_block:
                current_block.append(line.strip())
            elif FAILURE_PATTERN.search(line):
                # Start block even if header not matched
                current_block = [line.strip()]
                current_meta = {"timestamp": "unknown", "task": "unknown"}

    # ✅ Add last block
    if current_block:
        log_blocks.append({
            "timestamp": current_meta["timestamp"],
            "task": current_meta["task"],
            "logs": current_block
        })

    if not log_blocks:
        return None

    logger.info(f"Extracted {len(log_blocks)} failed log blocks.")
    if DEBUG_MODE:
        logger.debug(f"Extracted failed blocks: {json.dumps(log_blocks, indent=2)}")

    return {
        "timestamp": log_blocks[0]["timestamp"],
        "task": log_blocks[0]["task"],
        "error_blocks": log_blocks
    }


# === Upload or merge logs to S3 ===
def upload_to_s3(json_object):
    """Append new failed logs to a per-DAG-run JSON file in S3."""
    dag_id = json_object.get("dag_id", "unknown_dag")
    run_id = json_object.get("run_id", "unknown_run")
    task_id = json_object.get("task_id", "unknown_task") 
    #now = datetime.now().astimezone()
    SL_TIMEZONE = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(SL_TIMEZONE)

    # Define merged file location per DAG run
    #key = f"{MWAA_ENVIRONMENT}{now.strftime('%Y-%m-%d')}/{dag_id}/errors.json"
    key = f"{MWAA_ENVIRONMENT}{now.strftime('%Y-%m-%d/%H')}/{dag_id}/errors.json"


    logger.info(f"Preparing to upload or merge logs to s3://{DESTINATION_BUCKET}/{key}")

    # Try to load existing file (if exists)
    existing_data = []
    try:
        response = s3_client.get_object(Bucket=DESTINATION_BUCKET, Key=key)
        existing_body = response["Body"].read().decode("utf-8")
        existing_json = json.loads(existing_body)
        existing_data = existing_json.get("error_blocks", [])
        logger.info(f"Loaded existing file with {len(existing_data)} error blocks.")
    except s3_client.exceptions.NoSuchKey:
        logger.info("No existing file found — creating new one.")
    except Exception as e:
        logger.warning(f"Failed to read existing S3 file (will overwrite): {str(e)}")

    # Merge error blocks
    new_blocks = json_object.get("error_blocks", [])
    merged_blocks = existing_data + new_blocks

    # Write back to S3
    merged_json = {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "log_group": json_object.get("log_group", ""),
        "airflow_environment": json_object.get("airflow_environment", "unknown"),
        "path_in_curated_bucket": MWAA_ENVIRONMENT,
        #"last_updated": now.isoformat(),
        "last_updated": now.strftime('%Y-%m-%d %H:%M:%S'),
        "error_blocks": merged_blocks
    }

    s3_client.put_object(
        Bucket=DESTINATION_BUCKET,
        Key=key,
        Body=json.dumps(merged_json, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(f"Uploaded merged error log file with {len(merged_blocks)} total error blocks.")
    return key


# === Lambda handler ===
def lambda_handler(event, context):
    try:
        logger.info("Lambda triggered with new CloudWatch log data.")
        logger.info(f"Environment: {MWAA_ENVIRONMENT}")
        logger.info(f"Destination bucket: {DESTINATION_BUCKET}")

        # Decode and unzip CW log event
        cw_data = event["awslogs"]["data"]
        compressed_payload = base64.b64decode(cw_data)
        uncompressed_payload = gzip.decompress(compressed_payload)
        payload = json.loads(uncompressed_payload)

        logger.info(f"Decoded {len(payload.get('logEvents', []))} log events.")

        if DEBUG_MODE:
            logger.debug("Decoded CloudWatch payload:")
            logger.debug(json.dumps(payload, indent=2))

        # Extract Airflow metadata from log stream name
        log_stream = payload.get("logStream", "")
        airflow_meta = extract_airflow_metadata(log_stream)
        logger.info(f"Extracted metadata: {json.dumps(airflow_meta)}")

        # Collect log lines
        messages = [log_event["message"] for log_event in payload["logEvents"]]
        if DEBUG_MODE:
            logger.debug(f"All decoded log messages ({len(messages)}):")
            logger.debug(json.dumps(messages, indent=2))

        # Extract failed logs (with exclusion applied)
        failed_data = extract_failed_logs(messages)
        if not failed_data:
            logger.info("No failed logs found in this batch.")
            return {"statusCode": 200, "body": json.dumps({"message": "No failed logs found."})}

        # Merge metadata into output
        failed_data.update(airflow_meta)
        failed_data["log_group"] = payload.get("logGroup", "")
        failed_data["log_stream"] = log_stream

        log_group = failed_data["log_group"]

        # Determine Airflow environment based on log group
        if "airflow-prod-dialog-airflow-01-Task" in log_group:
            airflow_environment = "new"
        else:
            airflow_environment = "old"

        failed_data["airflow_environment"] = airflow_environment



        # Upload to S3
        s3_key = upload_to_s3(failed_data)
        logger.info(f"Uploaded merged error logs for {airflow_meta['dag_id']} run {airflow_meta['run_id']}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Failed logs merged successfully",
                "s3_key": s3_key,
                "dag_id": failed_data.get("dag_id"),
                "run_id": failed_data.get("run_id"),
                "error_count": len(failed_data.get("error_blocks", []))
            })
        }

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "type": type(e).__name__
            })
        }
