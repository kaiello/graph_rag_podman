import json
import os
import boto3
import logging
import tempfile
import zipfile
import uuid
from typing import List
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter

# --- Configuration ---
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
bedrock_client = boto3.client("bedrock-runtime")

EMBED_MODEL_ID = os.getenv("BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
LLM_MODEL_ID = os.getenv("BEDROCK_LLM_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0")

# -------------------------------------------------------------------------
# 1. Chunking & Embedding Logic (Moved from App 1)
# -------------------------------------------------------------------------
class ChunkProcessor:
    def __init__(self, chunk_size=2048, chunk_overlap=128):
        # 1. Split by Markdown headers
        self.headers_to_split_on = [
            ("#", "Header 1"),
            ("##", "Header 2"),
            ("###", "Header 3"),
        ]
        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=self.headers_to_split_on, 
            strip_headers=False
        )
        
        # 2. Split recursively
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, 
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", " ", ""]
        )

    def get_embedding(self, text: str) -> List[float]:
        """Invokes AWS Titan Embeddings v2"""
        try:
            clean_text = text.replace("\n", " ").strip()
            if not clean_text:
                return []
            
            body = json.dumps({
                "inputText": clean_text,
                "dimensions": 1024,
                "normalize": True
            })
            
            response = bedrock_client.invoke_model(
                modelId=EMBED_MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=body
            )
            
            response_body = json.loads(response['body'].read())
            return response_body.get("embedding")
        except Exception as e:
            logger.error(f"Bedrock embedding failed: {e}")
            return []

    def create_chunks(self, markdown_text: str, base_filename: str) -> List[dict]:
        chunks_data = []
        
        # Split 1: Logical Sections
        header_splits = self.markdown_splitter.split_text(markdown_text)
        
        # Split 2: Token Limits
        final_docs = self.text_splitter.split_documents(header_splits)
        
        for idx, doc in enumerate(final_docs):
            chunk_text = doc.page_content
            if not chunk_text.strip():
                continue

            # Embed
            embedding = self.get_embedding(chunk_text)
            chunk_id = f"{base_filename}_chunk_{idx}"
            
            # Graph RAG Schema (kb_Content_Block)
            chunk_record = {
                "entity_type": "kb_Content_Block",
                "id": chunk_id,
                "type": "text",
                "text": chunk_text,
                "embeddings": embedding,
                "document_uri": base_filename,
                "metadata": doc.metadata # Includes header info
            }
            chunks_data.append(chunk_record)
            
        return chunks_data

# -------------------------------------------------------------------------
# 2. Graph Extraction Logic
# -------------------------------------------------------------------------
def extract_entities_relationships(chunk_text):
    prompt = f"""
    You are a graph database expert. Analyze the following text chunk and extract entities and relationships.
    Text: "{chunk_text}"
    
    Rules:
    1. Identify entities (Person, Organization, Location, Project).
    2. Identify relationships (WORKS_FOR, LOCATED_IN, RELATED_TO).
    3. Output JSON with "nodes" and "edges" keys.
    4. Do not output markdown fences.
    """
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0
    })

    try:
        response = bedrock_client.invoke_model(modelId=LLM_MODEL_ID, body=body)
        response_body = json.loads(response.get("body").read())
        content = response_body["content"][0]["text"]
        clean_json = content.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_json)
    except Exception as e:
        logger.error(f"LLM Extraction failed: {e}")
        return {"nodes": [], "edges": []}

# -------------------------------------------------------------------------
# 3. Handler
# -------------------------------------------------------------------------
def lambda_handler(event, context):
    logger.info("Starting Graph Builder Process...")
    
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Safety check
        if not key.endswith("full.md"):
            return
            
        logger.info(f"Processing: s3://{bucket}/{key}")

        # Derive paths
        # Input: processed/pdf/output_xyz/xyz_full.md
        # Output Dir: processed/pdf/output_xyz/
        output_prefix = os.path.dirname(key) 

        with tempfile.TemporaryDirectory() as temp_dir:
            local_input = os.path.join(temp_dir, "full.md")
            s3_client.download_file(bucket, key, local_input)
            
            with open(local_input, "r", encoding="utf-8") as f:
                content = f.read()

            # A. Process Chunks & Embeddings
            processor = ChunkProcessor()
            # Use filename stem for IDs
            base_name = os.path.basename(key).replace("_full.md", "")
            chunks = processor.create_chunks(content, base_name)
            
            logger.info(f"Generated {len(chunks)} embedded chunks")

            # B. Graph Extraction
            all_nodes = []
            all_edges = []
            
            for chunk in chunks:
                # Add source ID to graph nodes/edges later if needed
                graph_data = extract_entities_relationships(chunk['text'])
                all_nodes.extend(graph_data.get("nodes", []))
                all_edges.extend(graph_data.get("edges", []))

            # C. Write Outputs
            vectors_path = os.path.join(temp_dir, "vectors.jsonl")
            nodes_path = os.path.join(temp_dir, "nodes.jsonl")
            edges_path = os.path.join(temp_dir, "edges.jsonl")
            
            # Save Vectors
            with open(vectors_path, 'w') as f:
                for c in chunks: f.write(json.dumps(c) + "\n")

            # Save Graph Elements
            with open(nodes_path, 'w') as f:
                for n in all_nodes: f.write(json.dumps(n) + "\n")
            with open(edges_path, 'w') as f:
                for e in all_edges: f.write(json.dumps(e) + "\n")

            # D. Package ZIP
            zip_name = "neo4j_import_package.zip"
            zip_path = os.path.join(temp_dir, zip_name)
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(vectors_path, arcname="vectors.jsonl")
                z.write(nodes_path, arcname="nodes.jsonl")
                z.write(edges_path, arcname="edges.jsonl")

            # E. Upload to SAME FOLDER
            # Upload individual vectors file
            s3_client.upload_file(vectors_path, bucket, f"{output_prefix}/vectors.jsonl")
            # Upload Zip package
            s3_client.upload_file(zip_path, bucket, f"{output_prefix}/{zip_name}")
            
            logger.info(f"SUCCESS. Artifacts uploaded to {output_prefix}/")
            
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {str(e)}")
        raise e

    return {"statusCode": 200, "body": "Processing Complete"}