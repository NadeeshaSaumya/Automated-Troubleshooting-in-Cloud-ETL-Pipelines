import json
import boto3
import os
import time
from datetime import datetime
import uuid
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
BUCKET = os.environ['S3_BUCKET']


def update_session_json(session_id, prompt, response, response_time):
    now = datetime.utcnow()
    key = f"AI_Monitoring_Project/agent-sessions/{now.year}-{now.month:02d}-{now.day:02d}/{session_id}.json"

    try:
        # Try to read existing session
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        session_data = json.loads(obj['Body'].read().decode('utf-8'))

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
        # Create new session
            session_data = {
                "session_id": session_id,
                "created_at": datetime.utcnow().isoformat(),
                "interactions": []
            }
        else:
            raise

    # Append new interaction
    session_data["interactions"].append({
        "timestamp": datetime.utcnow().isoformat(),
        "prompt": prompt,
        "response": response,
        "response_time_ms": response_time
    })

    # Upload back to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(session_data)
    )


def lambda_handler(event, context):
    try:
        start_time = time.time()

        if isinstance(event.get('body', None), str):
            body = json.loads(event['body'])
        else:
            body = event

        print('body:',body)
        prompt = body.get('prompt', '')
        print('prompt:',prompt)
        session_id = body.get('conversation_id', '')
        print('session_id:',session_id)        
 

        if not session_id:
            session_id = str(uuid.uuid4())  # generate new session
        print('session_id:', session_id)
        
        if not prompt:
            return {
                'statusCode': 400,
                'body': json.dumps({'response': 'No prompt provided'})
            }

        bedrock_runtime = boto3.client(
            service_name='bedrock-agent-runtime',
            region_name=os.environ['REGION']
        )

        print(os.environ)

        response = bedrock_runtime.invoke_agent(
            agentId=os.environ['BEDROCK_AGENT_ID'],
            agentAliasId=os.environ['BEDROCK_AGENT_ALIAS_ID'],
            sessionId=session_id,
            inputText=prompt,
            enableTrace=False
        )

        agent_response = ""

        for event_chunk in response.get("completion", []):
            chunk = event_chunk["chunk"]
            agent_response += chunk["bytes"].decode()
            print('Agent response:', agent_response)
            
        response_time = int((time.time() - start_time) * 1000)

        # Update session file
        update_session_json(session_id, prompt, agent_response, response_time)

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # For CORS support
            },
            'body': json.dumps({
                'response': agent_response,
                'session_id': session_id
    })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'response': str(e)})
        }
        
        