import json
import os
import boto3
from smart_open import open as smart_open
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

# Initialize structured logging
logger = Logger(service="vector_splitter")

# Get the destination bucket from Environment Variables (set in template.yaml)
FINAL_OUTPUT_BUCKET = os.environ.get("FINAL_OUTPUT_BUCKET")

def lambda_handler(event, context: LambdaContext):
    """
    Triggered by S3 ObjectCreated event.
    Splits the input JSONL into:
      1. Vectors (JSONL)
      2. Content/Metadata (Gzipped JSONL)
    """
    try:
        # 1. Parse S3 Event
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        source_key = record['s3']['object']['key']
        
        logger.info(f"Received event for: s3://{source_bucket}/{source_key}")
        
        # 2. Construct S3 URIs
        input_uri = f"s3://{source_bucket}/{source_key}"
        
        # Extract filename (e.g., "uploads/doc1.jsonl" -> "doc1")
        base_filename = os.path.splitext(os.path.basename(source_key))[0]
        
        # Define Output URIs
        # Output A: Vectors (Standard JSONL)
        vector_key = f"vectors/{base_filename}_vectors.jsonl"
        vector_uri = f"s3://{FINAL_OUTPUT_BUCKET}/{vector_key}"
        
        # Output B: Content (Compressed JSONL)
        # smart_open detects .gz extension and compresses automatically
        content_key = f"content/{base_filename}_content.jsonl.gz"
        content_uri = f"s3://{FINAL_OUTPUT_BUCKET}/{content_key}"

        # 3. Perform Streaming Split
        process_streams(input_uri, vector_uri, content_uri)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Splitting complete",
                "vectors": vector_uri,
                "content": content_uri
            })
        }

    except Exception as e:
        logger.exception("Failed to process file")
        raise e  # Raise to trigger Lambda retry behavior

def process_streams(input_uri, vector_uri, content_uri):
    """
    Reads from Input stream, writes to two Output streams simultaneously.
    Uses smart_open for memory-efficient streaming.
    """
    logger.info("Starting stream processing...")
    
    count = 0
    
    # Open all streams at once using Context Managers
    # 'r' = read text, 'w' = write text (smart_open handles encoding and compression)
    with smart_open(input_uri, 'r', encoding='utf-8') as fin, \
         smart_open(vector_uri, 'w', encoding='utf-8') as f_vec, \
         smart_open(content_uri, 'w', encoding='utf-8') as f_content:
        
        for line in fin:
            if not line.strip():
                continue
            
            try:
                data = json.loads(line)
                
                # --- Output A: Prepare Vector Entry ---
                # Safely get embedding (handle 'embedding_vector' vs 'embeddings')
                emb_vector = data.get("embedding_vector") or data.get("embeddings")
                
                if emb_vector:
                    vector_entry = {
                        "id": data.get("id"),
                        "chunk_id": data.get("chunk_id"), # Ensure chunk_id is carried over
                        "embedding_vector": emb_vector
                    }
                    f_vec.write(json.dumps(vector_entry) + '\n')
                
                # --- Output B: Prepare Content Entry ---
                # Create a copy and remove the heavy vector
                clean_entry = data.copy()
                clean_entry.pop("embedding_vector", None)
                clean_entry.pop("embeddings", None)
                
                # Write to content stream (will be auto-gzipped)
                f_content.write(json.dumps(clean_entry) + '\n')
                
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Processed {count} chunks...")

            except json.JSONDecodeError:
                logger.warning(f"Skipping invalid JSON on line {count + 1}")
                continue
                
    logger.info(f"Finished splitting. Total chunks: {count}")