import json
import os
import boto3
import urllib.parse

s3 = boto3.client('s3')

def process_s3_file(source_bucket, s3_key):
    # Get the target output bucket from environment variables
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    
    if not output_bucket:
        print("Error: OUTPUT_BUCKET environment variable not set.")
        return

    # 1. Define Paths
    filename = os.path.basename(s3_key)
    base_name = os.path.splitext(filename)[0]

    local_input = f"/tmp/{filename}"
    local_text = f"/tmp/{base_name}_text.jsonl"
    local_vector = f"/tmp/{base_name}_vectors.jsonl"

    output_prefix = f"{base_name}_split"
    s3_text_key = f"{output_prefix}/{base_name}_text.jsonl"
    s3_vector_key = f"{output_prefix}/{base_name}_vectors.jsonl"

    print(f"Processing: s3://{source_bucket}/{s3_key}")

    try:
        # 2. Download
        s3.download_file(source_bucket, s3_key, local_input)

        # 3. Process
        count = 0
        with open(local_input, 'r', encoding='utf-8') as infile, \
             open(local_text, 'w', encoding='utf-8') as f_text, \
             open(local_vector, 'w', encoding='utf-8') as f_vec:
            
            for line in infile:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    
                    # --- LOGIC UPDATE START ---
                    # Identify which key holds the vector
                    vector_key = None
                    if "embedding_vector" in data:
                        vector_key = "embedding_vector"
                    elif "embeddings" in data:
                        vector_key = "embeddings"
                    
                    # Extract the vector data (or None if not found)
                    vector_data = data.get(vector_key) if vector_key else None
                    
                    # 1. Prepare Vector Entry (Standardized to 'embedding_vector')
                    vector_entry = {
                        "id": data.get("id"),
                        "embedding_vector": vector_data
                    }
                    
                    # 2. Prepare Clean Entry (Remove whichever key was found)
                    clean_entry = data.copy()
                    if vector_key and vector_key in clean_entry:
                        del clean_entry[vector_key]
                    # --- LOGIC UPDATE END ---

                    f_text.write(json.dumps(clean_entry) + '\n')
                    f_vec.write(json.dumps(vector_entry) + '\n')
                    count += 1
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON on line {count + 1}")

        # 4. Upload
        print(f"Uploading output files to {output_bucket}...")
        s3.upload_file(local_text, output_bucket, s3_text_key)
        s3.upload_file(local_vector, output_bucket, s3_vector_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Success! Processed {count} chunks into {output_bucket}/{output_prefix}')
        }

    except Exception as e:
        print(f"Error: {e}")
        raise e

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        
        process_s3_file(bucket, key)