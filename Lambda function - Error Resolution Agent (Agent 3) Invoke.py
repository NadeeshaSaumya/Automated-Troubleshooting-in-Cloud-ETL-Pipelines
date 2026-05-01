import json
import boto3
import os

# === AWS Clients ===
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
bedrock_agent_client = boto3.client("bedrock-agent-runtime")

# === Environment Variables (configure these in Lambda console) ===
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
AGENT_ID = os.environ["AGENT_ID"]            # Your Bedrock Agent ID
AGENT_ALIAS_ID = os.environ["AGENT_ALIAS_ID"]  # Your Bedrock Agent alias ID
REGION = os.environ["REGION"]

def lambda_handler(event, context):
    """
    Triggered when an error file is uploaded to the specified S3 bucket.
    Reads the JSON file, sends it to the Bedrock agent, and publishes
    the agent's response to an SNS topic.
    """

    try:
        # 1️⃣ Get file details from S3 event
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]

        print(f"Received file: s3://{bucket_name}/{file_key}")

        # 2️⃣ Read error log file content
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = s3_response["Body"].read().decode("utf-8")
        error_data = json.loads(file_content)

        # 3️⃣ Prepare input text for the Bedrock agent
        input_text = json.dumps(error_data, indent=2)

        # 4️⃣ Invoke Bedrock Agent
        print("Invoking Bedrock Agent...")
        response = bedrock_agent_client.invoke_agent(
            agentId=AGENT_ID,
            agentAliasId=AGENT_ALIAS_ID,
            sessionId=context.aws_request_id,  # unique session per Lambda execution
            inputText=input_text
        )

        # 5️⃣ Extract the agent response
        completion = ""
        for event_item in response["completion"]:
            if "chunk" in event_item:
                completion += event_item["chunk"]["bytes"].decode("utf-8")

        print("Agent Response:")
        print(completion)

        # 6️⃣ Publish to SNS topic
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Airflow Error Resolution Notification",
            Message=f"Error File: {file_key}\n\nBedrock Agent Output:\n{completion}"
        )

        print("Notification sent successfully.")
        return {"status": "success", "message": "Processed and notified"}

    except Exception as e:
        print(f"Error processing file: {e}")
        raise e
