import json
import os
import boto3
import yaml
import logging
import urllib.parse
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')

def load_semantic_contract(yaml_path="schema_contract.yaml"):
    if not os.path.exists(yaml_path):
        logger.error(f"Schema not found at: {yaml_path}")
        raise FileNotFoundError(f"Schema not found")
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def extract_graph_from_text(text_chunk, schema):
    model_id = os.environ.get("LLM_MODEL", "amazon.titan-text-express-v1")
    
    prompt = f"""You are a Knowledge Graph Extraction expert. 
Extract nodes and edges from the provided text according to the following schema:
{json.dumps(schema, indent=2)}

Input Text:
{text_chunk}

Instructions:
1. Identify entities and relationships defined in the schema.
2. Return the output STRICTLY as a JSON object.
3. Do not add any conversational text, markdown formatting, or explanations. 
4. The format must be:
{{
  "nodes": [ {{"id": "...", "label": "...", "properties": {{...}} }} ],
  "edges": [ {{"source": "...", "target": "...", "relationship": "...", "properties": {{...}} }} ]
}}

JSON Output:
"""
    body = json.dumps({
        "inputText": prompt,
        "textGenerationConfig": {
            "maxTokenCount": 2048,
            "stopSequences": [],
            "temperature": 0.0,
            "topP": 1
        }
    })

    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            body=body,
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        output_text = response_body.get("results")[0].get("outputText")
        output_text = output_text.replace("```json", "").replace("```", "").strip()

        # Additional logging for debugging
        logger.info(f"Raw LLM Output: {output_text}")
        return json.loads(output_text)

    except Exception as e:
        logger.error(f"Extraction Error: {e}")
        return {"nodes": [], "edges": []}

def process_file(source_bucket, key):
    graph_bucket = os.environ.get('GRAPH_BUCKET')
    if not graph_bucket:
        logger.error("GRAPH_BUCKET env var is missing.")
        return

    filename = os.path.basename(key)
    if filename.endswith("_text.jsonl"):
        base_name = filename.replace("_text.jsonl", "")
    else:
        base_name = os.path.splitext(filename)[0]

    output_key = f"{base_name}_graph_data.jsonl"
    local_input = f"/tmp/{filename}"
    local_output = f"/tmp/{output_key}"

    try:
        schema = load_semantic_contract()
        logger.info(f"Downloading {source_bucket}/{key}")
        s3.download_file(source_bucket, key, local_input)
    except Exception as e:
        logger.error(f"Setup Error: {e}")
        return

    processed_count = 0
    with open(local_input, 'r', encoding='utf-8') as infile, \
         open(local_output, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            if not line.strip(): continue
            try:
                record = json.loads(line)
                text_content = record.get("text", "")
                if not text_content: continue

                graph_data = extract_graph_from_text(text_content, schema)
                
                output_record = {
                    "source_id": record.get("id"),
                    "page_number": record.get("page_number"),
                    "extracted_nodes": graph_data.get("nodes", []),
                    "extracted_edges": graph_data.get("edges", [])
                }
                outfile.write(json.dumps(output_record) + '\n')
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing chunk: {e}")

    logger.info(f"Uploading results to s3://{graph_bucket}/{output_key}")
    s3.upload_file(local_output, graph_bucket, output_key)

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        process_file(bucket, key)