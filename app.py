#!/usr/bin/env python3
"""
Integrated Docling Extractor v5.6 (Lambda Version)
Features:
1. S3 Triggered: Processes files uploaded to S3.
2. Multi-Format: Supports PDF, Images, DOCX, PPTX, HTML, MD.
3. Page Splitting: Exports Markdown, JSON, and Figures SEPARATELY for each page/slide.
4. Header Snapping: Automatically expands figures to include the Slide/Section Title.
5. Smart Figure Merging: Reconstructs "Quads" from fragmented detections.
6. Safety Checks: Skips locked files and sanitizes filenames.
"""
from __future__ import annotations
import logging
import time
import re
import json
import os
import shutil
import boto3
from pathlib import Path
from typing import List, Tuple, Dict, Any
from urllib.parse import unquote_plus

# Data Handling
from PIL import Image
import pandas as pd

# Docling Core
from docling.document_converter import (
    DocumentConverter, 
    PdfFormatOption, 
    ImageFormatOption, 
    WordFormatOption,
    PowerpointFormatOption,
    HTMLFormatOption
)
from docling.datamodel.base_models import InputFormat, ConversionStatus
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions, 
    TableFormerMode, 
    TesseractCliOcrOptions,
    VlmPipelineOptions
)
from docling.datamodel.document import (
    DoclingDocument, 
    TableItem, 
    TextItem, 
    SectionHeaderItem, 
    ListItem
)
# VLM Support (Optional)
from docling.pipeline.vlm_pipeline import VlmPipeline

# Unstructured Support
from unstructured.documents.elements import Text, Table, Title, ListItem as UnstructuredListItem, ElementMetadata
from unstructured.staging.base import elements_to_json

# Initialize S3 Client
s3_client = boto3.client('s3')

def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")
    return logging.getLogger(__name__)

_log = setup_logging(verbose=True)

# -------------------------------------------------------------------------
# 1. UTILITIES
# -------------------------------------------------------------------------
def sanitize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name

# (Removed filter_accessible_files as it is not needed for S3 processing)

# -------------------------------------------------------------------------
# 2. PIPELINE CONFIGURATION
# -------------------------------------------------------------------------
def get_configured_converter(use_vlm: bool = False) -> DocumentConverter:
    allowed_formats = [
        InputFormat.PDF, InputFormat.IMAGE, 
        InputFormat.DOCX, InputFormat.PPTX, 
        InputFormat.HTML, InputFormat.MD, InputFormat.ASCIIDOC
    ]

    format_options = {}

    if use_vlm:
        _log.info("🚀 Initializing VLM Pipeline (PDF/Image only)...")
        pipeline_options = VlmPipelineOptions()
        format_options[InputFormat.PDF] = PdfFormatOption(pipeline_cls=VlmPipeline, pipeline_options=pipeline_options)
        format_options[InputFormat.IMAGE] = ImageFormatOption(pipeline_cls=VlmPipeline, pipeline_options=pipeline_options)
    else:
        _log.info("🔧 Initializing Standard Pipeline (High-Res PDF/Img, Standard Office)...")
        
        pdf_options = PdfPipelineOptions()
        pdf_options.do_table_structure = True
        pdf_options.table_structure_options.mode = TableFormerMode.ACCURATE
        pdf_options.do_ocr = True
        pdf_options.ocr_options = TesseractCliOcrOptions()
        pdf_options.images_scale = 2.0 
        pdf_options.generate_page_images = True 
        pdf_options.generate_picture_images = True

        format_options[InputFormat.PDF] = PdfFormatOption(pipeline_options=pdf_options)
        format_options[InputFormat.IMAGE] = ImageFormatOption(pipeline_options=pdf_options)
        
        format_options[InputFormat.DOCX] = WordFormatOption()
        format_options[InputFormat.PPTX] = PowerpointFormatOption()
        format_options[InputFormat.HTML] = HTMLFormatOption()

    return DocumentConverter(
        allowed_formats=allowed_formats,
        format_options=format_options
    )

# -------------------------------------------------------------------------
# 3. GEOMETRY & MERGING LOGIC
# -------------------------------------------------------------------------
def merge_nearby_bboxes(bboxes, distance_threshold=50):
    if not bboxes:
        return []
    merged = []
    working_set = list(bboxes)
    while working_set:
        current = working_set.pop(0)
        changed = True
        while changed:
            changed = False
            rest = []
            for other in working_set:
                h_overlap = (current[0] <= other[2] + distance_threshold) and (other[0] <= current[2] + distance_threshold)
                v_overlap = (current[1] <= other[3] + distance_threshold) and (other[1] <= current[3] + distance_threshold)
                if h_overlap and v_overlap:
                    current = (
                        min(current[0], other[0]),
                        min(current[1], other[1]),
                        max(current[2], other[2]),
                        max(current[3], other[3])
                    )
                    changed = True
                else:
                    rest.append(other)
            working_set = rest
        merged.append(current)
    return merged

def include_header_context(doc: DoclingDocument, page_no: int, bbox: Tuple[float, float, float, float], max_distance: int = 200) -> Tuple[float, float, float, float]:
    x0, y0, x1, y1 = bbox
    best_header_y = y0
    for item in doc.texts:
        if not (hasattr(item, "prov") and item.prov and item.prov[0].page_no == page_no):
            continue
        if isinstance(item, SectionHeaderItem) or isinstance(item, TextItem):
            h_bbox = item.prov[0].bbox.as_tuple()
            h_x0, h_y0, h_x1, h_y1 = h_bbox
            if h_y1 < y0:
                dist = y0 - h_y1
                if dist < max_distance:
                    if h_y0 < best_header_y:
                        best_header_y = h_y0
    return (x0, best_header_y, x1, y1)

def add_padding(bbox, width, height, padding=15):
    return (
        max(0, bbox[0] - padding),
        max(0, bbox[1] - padding),
        min(width, bbox[2] + padding),
        min(height, bbox[3] + padding)
    )

# -------------------------------------------------------------------------
# 4. ASSET EXPORT (Global & Per-Page)
# -------------------------------------------------------------------------
def export_enhanced_assets(doc: DoclingDocument, output_dir: Path, base_name: str):
    tables_dir = output_dir / "tables"
    figures_dir = output_dir / "figures"
    tables_dir.mkdir(exist_ok=True)
    figures_dir.mkdir(exist_ok=True)

    # Tables (Global export)
    for i, table in enumerate(doc.tables):
        try:
            # Export dataframe once
            df = table.export_to_dataframe(doc)
            
            # 1. Save as CSV
            df.to_csv(tables_dir / f"{base_name}_table_{i+1}.csv", index=False)
            
            # 2. Save as Markdown (Requires 'tabulate' package)
            # We use a try-except block here specifically for markdown in case tabulate is missing
            try:
                with open(tables_dir / f"{base_name}_table_{i+1}.md", "w", encoding="utf-8") as f:
                    f.write(df.to_markdown(index=False))
            except ImportError:
                _log.warning(f"Could not export Table {i+1} to Markdown. Please install 'tabulate' (pip install tabulate).")
            except Exception as e:
                _log.debug(f"Failed to export markdown for table {i+1}: {e}")

        except Exception as e:
             _log.debug(f"Failed to export table {i+1} to CSV: {e}")

    # Figures (Iterate per page)
    for page_no, page in doc.pages.items():
        if not (page.image and page.image.pil_image):
            continue
        
        full_page_img = page.image.pil_image
        page_w, page_h = full_page_img.size
        
        page_bboxes = []
        for picture in doc.pictures:
            if picture.prov and picture.prov[0].page_no == page_no:
                page_bboxes.append(picture.prov[0].bbox.as_tuple())
        
        merged_bboxes = merge_nearby_bboxes(page_bboxes, distance_threshold=50)
        
        for i, bbox in enumerate(merged_bboxes):
            w, h = bbox[2]-bbox[0], bbox[3]-bbox[1]
            if w < 150 or h < 150: continue

            bbox_with_header = include_header_context(doc, page_no, bbox)
            final_bbox = add_padding(bbox_with_header, page_w, page_h, padding=20)
            
            try:
                crop = full_page_img.crop(final_bbox)
                crop.save(figures_dir / f"{base_name}_pg{page_no}_smart_fig_{i+1}.png")
            except Exception as e:
                _log.warning(f"Crop failed on page {page_no}: {e}")

# -------------------------------------------------------------------------
# 5. DATA MAPPING
# -------------------------------------------------------------------------
def map_docling_to_unstructured(docling_doc: DoclingDocument) -> List[dict]:
    unstructured_elements = []
    for item, level in docling_doc.iterate_items():
        metadata = ElementMetadata()
        if hasattr(item, "prov") and item.prov:
            metadata.page_number = item.prov[0].page_no

        if isinstance(item, TableItem):
            try:
                csv = item.export_to_dataframe(doc=docling_doc).to_csv(index=False)
                html = item.export_to_html(doc=docling_doc)
                metadata.text_as_html = html
                unstructured_elements.append(Table(text=csv, metadata=metadata))
            except: pass
        elif isinstance(item, SectionHeaderItem):
            unstructured_elements.append(Title(text=item.text, metadata=metadata))
        elif isinstance(item, ListItem):
            unstructured_elements.append(UnstructuredListItem(text=item.text, metadata=metadata))
        elif isinstance(item, TextItem):
            unstructured_elements.append(Text(text=item.text, metadata=metadata))
            
    return unstructured_elements

# -------------------------------------------------------------------------
# 6. SPLIT BY PAGE LOGIC
# -------------------------------------------------------------------------
def save_per_page_results(doc: DoclingDocument, output_root: Path, base_name: str, pretty: bool):
    """
    Splits the document and saves artifacts into a 'pages/page_X' folder structure.
    """
    pages_root = output_root / "pages"
    pages_root.mkdir(exist_ok=True)

    # Iterate over all pages found in the document
    for page_no in doc.pages.keys():
        page_dir = pages_root / f"page_{page_no}"
        page_dir.mkdir(exist_ok=True)

        # 1. Filter Elements for this page
        page_elements = []
        page_md_lines = []
        
        # Iterate items again to filter by page
        for item, level in doc.iterate_items():
            if not (hasattr(item, "prov") and item.prov and item.prov[0].page_no == page_no):
                continue
            
            # Markdown Construction (Simple Approximation)
            if isinstance(item, SectionHeaderItem):
                page_md_lines.append(f"## {item.text}\n")
            elif isinstance(item, ListItem):
                page_md_lines.append(f"- {item.text}")
            elif isinstance(item, TextItem):
                page_md_lines.append(f"{item.text}\n")
            elif isinstance(item, TableItem):
                page_md_lines.append(f"\n[TABLE ON PAGE {page_no}]\n")

            # Unstructured Element Construction
            metadata = ElementMetadata(page_number=page_no)
            if isinstance(item, TableItem):
                try:
                    csv = item.export_to_dataframe(doc=doc).to_csv(index=False)
                    page_elements.append(Table(text=csv, metadata=metadata))
                except: pass
            elif isinstance(item, SectionHeaderItem):
                page_elements.append(Title(text=item.text, metadata=metadata))
            elif isinstance(item, ListItem):
                page_elements.append(UnstructuredListItem(text=item.text, metadata=metadata))
            elif isinstance(item, TextItem):
                page_elements.append(Text(text=item.text, metadata=metadata))

        # 2. Save Per-Page Markdown
        with open(page_dir / f"{base_name}_pg{page_no}.md", "w", encoding="utf-8") as f:
            f.write("\n".join(page_md_lines))

        # 3. Save Per-Page JSON
        with open(page_dir / f"{base_name}_pg{page_no}.json", "w", encoding="utf-8") as f:
            f.write(elements_to_json(page_elements, indent=2 if pretty else None))

    _log.info(f"   ↳ Split content into {len(doc.pages)} page(s) inside '{pages_root}'")


# (Removed get_input_files)

def save_result(result, output_root: Path, pretty: bool):
    file_path = result.input.file
    
    if result.status != ConversionStatus.SUCCESS:
        _log.error(f"❌ Conversion failed for: {file_path.name} (Status: {result.status})")
        if result.errors:
            for err in result.errors:
                _log.error(f"   - {err}")
        return

    try:
        clean_stem = sanitize_filename(file_path.stem)
        file_output_dir = output_root / f"output_{clean_stem}"
        file_output_dir.mkdir(parents=True, exist_ok=True)

        doc = result.document
        
        # 1. Export Full Markdown
        with open(file_output_dir / f"{clean_stem}_full.md", "w", encoding="utf-8") as f:
            f.write(doc.export_to_markdown())
        
        # 2. Export Full JSON
        elements = map_docling_to_unstructured(doc)
        with open(file_output_dir / f"{clean_stem}_full.json", "w", encoding="utf-8") as f:
            f.write(elements_to_json(elements, indent=2 if pretty else None))

        # 3. Export Assets
        export_enhanced_assets(doc, file_output_dir, clean_stem)

        # 4. NEW: Export Split Pages
        save_per_page_results(doc, file_output_dir, clean_stem, pretty)
        
        _log.info(f"✅ Saved: {file_path.name} -> {file_output_dir}")

    except Exception as e:
        _log.error(f"⚠️ Error saving results for {file_path.name}: {e}", exc_info=True)

# -------------------------------------------------------------------------
# 7. LAMBDA HANDLER
# -------------------------------------------------------------------------
def upload_directory_to_s3(local_dir: Path, bucket: str, s3_prefix: str):
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = Path(root) / file
            relative_path = local_path.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
            
            _log.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
            s3_client.upload_file(str(local_path), bucket, s3_key)

def lambda_handler(event, context):
    # Configuration from Environment Variables
    use_vlm = os.environ.get('USE_VLM', 'false').lower() == 'true'
    pretty_json = os.environ.get('PRETTY_JSON', 'false').lower() == 'true'
    
    # Temporary directories
    tmp_root = Path("/tmp")
    input_dir = tmp_root / "input"
    output_dir = tmp_root / "output"
    
    # Clean up /tmp from previous executions (if warm start)
    if input_dir.exists(): shutil.rmtree(input_dir)
    if output_dir.exists(): shutil.rmtree(output_dir)
    input_dir.mkdir()
    output_dir.mkdir()

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        
        _log.info(f"Processing file: s3://{bucket}/{key}")
        
        local_filename = Path(key).name
        local_input_path = input_dir / local_filename
        
        try:
            s3_client.download_file(bucket, key, str(local_input_path))
        except Exception as e:
            _log.error(f"Failed to download {key} from {bucket}: {e}")
            continue

        converter = get_configured_converter(use_vlm=use_vlm)
        
        _log.info("🔄 Starting Conversion...")
        start_time = time.time()
        
        # Convert single file
        results = converter.convert_all([local_input_path], raises_on_error=False)
        
        for result in results:
            save_result(result, output_dir, pretty_json)

        total_time = time.time() - start_time
        _log.info(f"🎉 Conversion complete in {total_time:.2f}s")
        
        # Upload results back to S3
        output_bucket = os.environ.get('OUTPUT_BUCKET', bucket)
        output_prefix = f"processed/{Path(key).stem}"
        upload_directory_to_s3(output_dir, output_bucket, output_prefix)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }