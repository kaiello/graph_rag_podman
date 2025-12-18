import json
import os
import boto3
import urllib.parse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Configuration
VECTOR_BUCKET = os.environ.get('VECTOR_BUCKET')  # e.g., "my-docling-output-artifacts-2025"
FINAL_BUCKET = os.environ.get('FINAL_BUCKET', 'graph_data_final')

def load_vectors_into_map(bucket, key):
    """
    Downloads the vector file and creates a map: {id: {embeddings: [...], text: "..."}}
    """
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, local_path)
    
    vector_map = {}
    with open(local_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            data = json.loads(line)
            # Map by ID. Adjust key if your vector file uses 'source_id' or 'id'
            rec_id = data.get('id') or data.get('source_id')
            if rec_id:
                # We only need the fields that were missing from graph_data
                vector_map[rec_id] = {
                    "embeddings": data.get("embeddings"),
                    "text": data.get("text"),
                    "metadata": data.get("metadata", {})
                }
    return vector_map

def merge_datasets(graph_path, vector_map, output_path):
    with open(graph_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            if not line.strip(): continue
            graph_record = json.loads(line)
            
            # The extractor output likely uses 'source_id' based on your previous code
            rec_id = graph_record.get('source_id')
            
            if rec_id and rec_id in vector_map:
                vector_data = vector_map[rec_id]
                
                # MERGE LOGIC: Combine graph nodes/edges with original text/vectors
                merged_record = {
                    "id": rec_id,
                    "text": vector_data.get("text"),
                    "metadata": vector_data.get("metadata"),
                    "embeddings": vector_data.get("embeddings"),
                    "graph": {
                        "nodes": graph_record.get("extracted_nodes", []),
                        "edges": graph_record.get("extracted_edges", [])
                    }
                }
                outfile.write(json.dumps(merged_record) + '\n')
            else:
                logger.warning(f"ID {rec_id} found in graph data but missing in vector file.")

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        
        # 1. Identify filenames
        filename = os.path.basename(key)
        # Expecting: sea_spyder_quad_embeddings_a_graph_data.jsonl
        # Target Vector: sea_spyder_quad_embeddings_a.jsonl (or similar pattern)
        
        # Heuristic to find the vector file name based on your naming convention
        # You might need to adjust this replace logic to match your exact file names
        vector_key = key.replace("_graph_data.jsonl", ".jsonl") 
        # Or if vectors are in a folder: 
        # vector_key = f"sea_spyder_quad_embeddings_a_split/{filename.replace('_graph_data.jsonl', '.jsonl')}"

        logger.info(f"Processing {filename}, looking for vectors at {VECTOR_BUCKET}/{vector_key}")

        local_graph = f"/tmp/{filename}"
        local_output = f"/tmp/{filename.replace('_graph_data', '_graph_upload')}"

        try:
            # 2. Download Graph Data (Trigger File)
            s3.download_file(source_bucket, key, local_graph)
            
            # 3. Download and Load Vectors
            # NOTE: This assumes the vector file is available. 
            # If the vector file is huge, we might need a streaming join, but a map is fine for <500MB
            vector_map = load_vectors_into_map(VECTOR_BUCKET, vector_key)
            
            # 4. Merge
            merge_datasets(local_graph, vector_map, local_output)
            
            # 5. Upload Final
            final_key = os.path.basename(local_output)
            s3.upload_file(local_output, FINAL_BUCKET, final_key)
            logger.info(f"Successfully uploaded merged file to {FINAL_BUCKET}/{final_key}")

        except Exception as e:
            logger.error(f"Merge failed: {e}")
            raise e