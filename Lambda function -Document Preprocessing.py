import boto3
import json
import os
import pandas as pd
from PyPDF2 import PdfReader
from docx import Document
import uuid
import time
from urllib.parse import unquote_plus  # Utility for handling URL-encoded keys
import logging

# -------------------------------
# Setup logging
# -------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client("s3")
textract = boto3.client("textract")

# -------------------------------
# Utility: Chunk text into ~300 words
# -------------------------------
def chunk_text(text, max_words=300):
    """Generates text chunks of a maximum word count."""
    words = text.split()
    for i in range(0, len(words), max_words):
        yield " ".join(words[i:i + max_words])

# -------------------------------
# Helper: parse Textract response into lines + simple table rows
# -------------------------------
def parse_textract_blocks(blocks):
    blocks_map = {b['Id']: b for b in blocks}
    lines = [b['Text'] for b in blocks if b.get('BlockType') == 'LINE' and 'Text' in b]

    table_rows_strings = []
    for b in blocks:
        if b.get('BlockType') == 'TABLE':
            child_ids = []
            if 'Relationships' in b:
                for rel in b['Relationships']:
                    if rel['Type'] == 'CHILD':
                        child_ids.extend(rel['Ids'])

            table_map = {}
            for cell_id in child_ids:
                cell = blocks_map.get(cell_id)
                if not cell or cell.get('BlockType') != 'CELL':
                    continue
                row = cell.get('RowIndex', 0)
                col = cell.get('ColumnIndex', 0)

                cell_text_parts = []
                if 'Relationships' in cell:
                    for rel in cell['Relationships']:
                        if rel['Type'] == 'CHILD':
                            for cid in rel['Ids']:
                                child = blocks_map.get(cid)
                                if not child:
                                    continue
                                if child.get('BlockType') in ('WORD', 'LINE'):
                                    txt = child.get('Text', '')
                                    if txt:
                                        cell_text_parts.append(txt)
                                elif child.get('BlockType') == 'SELECTION_ELEMENT':
                                    if child.get('SelectionStatus') == 'SELECTED':
                                        cell_text_parts.append('[X]')
                cell_text = " ".join(cell_text_parts).strip()
                table_map.setdefault(row, {})[col] = cell_text

            for r in sorted(table_map.keys()):
                cols = table_map[r]
                row_parts = [cols[c] for c in sorted(cols.keys())]
                table_rows_strings.append(" | ".join(row_parts))

    return lines + table_rows_strings

# -------------------------------
# Process Tabular files (CSV / Excel)
# -------------------------------
def process_tabular_file(file_path, file_name):
    try:
        # Read all sheets if Excel, or single sheet if CSV
        if file_name.lower().endswith(".csv"):
            df = pd.read_csv(file_path)
            sheet_dict = {"__csv__": df}
        else:
            sheet_dict = pd.read_excel(file_path, sheet_name=None)

        content = []
        for sheet_name, data in sheet_dict.items():
            if data.empty:
                continue

            # Convert entire sheet to text
            sheet_text = ""
            for idx, row in data.iterrows():
                row_text = " | ".join([f"{col}: {str(row[col])}" for col in data.columns])
                sheet_text += row_text + "\n"

            content.append({
                "type": "table",
                "chunks": [{
                    "chunk_id": f"{sheet_name}",
                    "text": sheet_text.strip(),
                    "metadata": {
                        "sheet": str(sheet_name),
                        "file_name": file_name,
                        "total_rows": len(data),
                        "columns": list(data.columns)
                    }
                }]
            })

        return content

    except Exception as e:
        logger.error(f"Error processing tabular file: {e}", exc_info=True)
        return []

# -------------------------------
# Textract async PDF processing
# -------------------------------
def process_pdf_with_textract_async(bucket, key, max_wait_seconds=120):
    try:
        response = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
        job_id = response['JobId']
        logger.info(f"Started Textract job: {job_id}")

        start = time.time()
        while time.time() - start < max_wait_seconds:
            time.sleep(2)
            status_resp = textract.get_document_text_detection(JobId=job_id)
            status = status_resp.get('JobStatus')
            logger.info(f"Textract job {job_id} status: {status}")

            if status == 'SUCCEEDED':
                all_blocks = []
                next_token = None
                while True:
                    if next_token:
                        page_resp = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
                    else:
                        page_resp = status_resp
                    
                    all_blocks.extend(page_resp.get('Blocks', []))
                    next_token = page_resp.get('NextToken')
                    if not next_token:
                        break
                return parse_textract_blocks(all_blocks)
            
            elif status == 'FAILED':
                logger.error("Textract async job failed", exc_info=True)
                return []
            
        logger.error("Textract polling timed out.")
        return []
        
    except Exception as e:
        logger.error(f"Error in Textract async process: {e}", exc_info=True)
        return []

# -------------------------------
# Process Text (PDF/DOCX/TXT)
# -------------------------------
def process_text_file(file_path, file_name, s3_bucket=None, s3_key=None):
    content = []
    try:
        if file_name.lower().endswith(".pdf"):
            reader = PdfReader(file_path)
            extracted_any = False
            for page_num, page in enumerate(reader.pages, start=1):
                try:
                    text = page.extract_text() or ""
                except Exception:
                    text = ""
                    
                if text and text.strip():
                    extracted_any = True
                    for i, chunk in enumerate(chunk_text(text)):
                        content.append({
                            "type": "text",
                            "chunks": [{
                                "chunk_id": f"page-{page_num}-chunk-{i}",
                                "text": chunk,
                                "metadata": {"page_number": page_num}
                            }]
                        })

            if not extracted_any and s3_bucket and s3_key:
                logger.info("No text found with PyPDF2 — using Textract async on S3 object.")
                textract_blocks = process_pdf_with_textract_async(s3_bucket, s3_key)
                
                for i, txt in enumerate(textract_blocks):
                    for j, c in enumerate(chunk_text(txt)):
                        content.append({
                            "type": "text",
                            "chunks": [{
                                "chunk_id": f"textract-block-{i}-chunk-{j}",
                                "text": c,
                                "metadata": {}
                            }]
                        })
            
            elif not extracted_any:
                logger.warning("Scanned PDF detected but no S3 path provided or Textract failed.")

        elif file_name.lower().endswith(".docx"):
            doc = Document(file_path)
            for i, para in enumerate(doc.paragraphs):
                if para.text.strip():
                    for j, chunk in enumerate(chunk_text(para.text)):
                        content.append({
                            "type": "text",
                            "chunks": [{
                                "chunk_id": f"para-{i}-chunk-{j}",
                                "text": chunk,
                                "metadata": {"paragraph_number": i}
                            }]
                        })

        elif file_name.lower().endswith(".txt"):
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
                for i, chunk in enumerate(chunk_text(text)):
                    content.append({
                        "type": "text",
                        "chunks": [{
                            "chunk_id": f"chunk-{i}",
                            "text": chunk,
                            "metadata": {}
                        }]
                    })
        return content
    except Exception as e:
        logger.error(f"Error processing text file: {e}", exc_info=True)
        return []

# -------------------------------
# Process Image using Textract
# -------------------------------
def process_image_file_s3(bucket, key):
    content = []
    try:
        resp = textract.analyze_document(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=['TABLES', 'FORMS']
        )
        blocks = resp.get('Blocks', [])
        lines_and_tables = parse_textract_blocks(blocks)

        for i, txt in enumerate(lines_and_tables):
            for j, chunk in enumerate(chunk_text(txt)):
                content.append({
                    "type": "text",
                    "chunks": [{
                        "chunk_id": f"img-{i}-chunk-{j}",
                        "text": chunk,
                        "metadata": {"source_file": key}
                    }]
                })
        return content
    except Exception as e:
        logger.error(f"Error processing image with Textract: {e}", exc_info=True)
        return []

# -------------------------------
# Lambda Handler
# -------------------------------
def lambda_handler(event, context):
    download_path = None
    SOURCE_DESTINATION_MAP = {
        "AI_Monitoring_Project/document_base/airflow_knowledge_base/": "AI_Monitoring_Project/knowledge_bases/airflow_knowledge_base/",
        "AI_Monitoring_Project/document_base/server_knowledge_base/": "AI_Monitoring_Project/knowledge_bases/server_knowledge_base/"
    }
    
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key_raw = event["Records"][0]["s3"]["object"]["key"]
        key = unquote_plus(key_raw)

        kb_prefix = None
        for source_part, destination_prefix in SOURCE_DESTINATION_MAP.items():
            if source_part in key:
                kb_prefix = destination_prefix
                break
        
        if kb_prefix is None:
            raise ValueError(f"S3 key '{key}' does not match any known processing path in SOURCE_DESTINATION_MAP.")

        file_name = os.path.basename(key)
        download_path = f"/tmp/{file_name}"
        document_id = str(uuid.uuid4())
        
        logger.info(f"Processing file: s3://{bucket}/{key} -> Output Prefix: {kb_prefix}")

        s3.download_file(bucket, key, download_path)
        
        ext = file_name.lower().split('.')[-1]
        
        if ext in ("csv", "xlsx", "xls"):
            content = process_tabular_file(download_path, file_name)
        elif ext in ("pdf", "docx", "txt"):
            content = process_text_file(download_path, file_name, s3_bucket=bucket, s3_key=key)
        elif ext in ("png", "jpg", "jpeg"):
            content = process_image_file_s3(bucket, key)
        else:
            logger.warning(f"Unsupported file type: {ext}")
            return {"status": "unsupported file type", "file": file_name}

        kb_doc = {
            "document_id": document_id,
            "source": {
                "file_name": file_name,
                "file_type": ext,
                "s3_key": key
            },
            "content": content
        }

        kb_bucket = os.environ.get('KB_BUCKET')
        if not kb_bucket:
            raise EnvironmentError("KB_BUCKET environment variable not set") 

        kb_key = f"{kb_prefix}{file_name}{document_id}.json"
        
        s3.put_object(
            Bucket=kb_bucket,
            Key=kb_key,
            Body=json.dumps(kb_doc, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Successfully saved document {document_id} to s3://{kb_bucket}/{kb_key}")

        return {"status": "success", "kb_file": kb_key}

    except Exception as e:
        logger.error(f"Fatal error during processing: {e}", exc_info=True)
        return {"status": "error", "message": str(e), "file": key if 'key' in locals() else 'unknown'}

    finally:
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
            logger.info(f"Cleaned up {download_path}")
