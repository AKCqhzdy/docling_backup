import os
import queue
import threading
import time
import json
from pathlib import Path
import uvicorn
import uuid
import asyncio
from fastapi import FastAPI, Form, HTTPException
from starlette.responses import JSONResponse
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    MyOcrOptions
)
from typing import Optional
from docling.document_converter import DocumentConverter, PdfFormatOption
import logging
import boto3
s3_client = boto3.client('s3')

task_queue = queue.Queue()
task_events = {}
task_results = {}

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)

docling_converter = None

def perform_ocr(input_s3_path: str, output_s3_path: str):
    global docling_converter
    assert docling_converter is not None

    logging.info(f"start to deal with: {input_s3_path}")
    
    def parse_s3_path(s3_path: str):
        s3_path = s3_path.replace("s3://", "")
        bucket, *key_parts = s3_path.split("/")
        return bucket, "/".join(key_parts)

    file_bucket, file_key = parse_s3_path(input_s3_path)
    input_file_path = INPUT_DIR / file_bucket / file_key
    input_file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3_client.download_file(
            Bucket=file_bucket,
            Key=file_key,
            Filename=str(input_file_path)
        )
        logging.info(f"download from s3 successfully: {input_s3_path}")
    except Exception as e:
        error_msg = f"Failed to download file from S3: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}


    output_bucket, output_key = parse_s3_path(output_s3_path)
    output_file_path = OUTPUT_DIR / output_bucket / output_key
    output_md_path = output_file_path.with_suffix(".md")
    output_json_path = output_file_path.with_suffix(".json")
    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        doc = docling_converter.convert(str(input_file_path)).document
        md_content = doc.export_to_markdown()
        
        with open(output_md_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(doc.export_to_dict(), f, ensure_ascii=False, indent=4)

        logging.info(f"task finish: {input_s3_path}")

    except Exception as e:
        error_msg = f"An error occurred when processing the file {input_s3_path}: {e}"
        logging.error(error_msg)
        return {"error": error_msg}
    
    try:
        s3_client.upload_file(
            Bucket=output_bucket,
            Key=output_key,
            Filename=str(output_md_path)
        )
        logging.info(f"upload from s3 successfully: {output_md_path}")
    except Exception as e:
        error_msg = f"Failed to upload file from S3: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

    try:
        s3_client.upload_file(
            Bucket=output_bucket,
            Key=f"{output_key}.md",
            Filename=str(output_md_path)
        )
        logging.info(f"upload from s3 successfully: {output_md_path}")
    except Exception as e:
        error_msg = f"Failed to upload file from S3: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}
    try:
        s3_client.upload_file(
            Bucket=output_bucket,
            Key=f"{output_key}.json",
            Filename=str(output_json_path)
        )
        logging.info(f"upload from s3 successfully: {output_json_path}")
    except Exception as e:
        error_msg = f"Failed to upload file from S3: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}
    
    return md_content

def worker_thread_func():
    while True:
        task_id, input_path, output_path = task_queue.get()
        result = perform_ocr(input_path, output_path)
        if task_id in task_events:
            task_results[task_id] = result
            task_events[task_id].set()
        task_queue.task_done()



app = FastAPI(title="Docling OCR Service")
@app.on_event("startup")
def startup_event():
    global docling_converter
    logging.info("initing DocumentConverter...")

    pipeline_options = PdfPipelineOptions()
    # ocr_options = MyOcrOptions(force_full_page_ocr=True) 
    ocr_options = EasyOcrOptions(force_full_page_ocr=True)
    
    pipeline_options.ocr_options = ocr_options

    docling_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )
    logging.info("DocumentConverter initialization done")

    worker = threading.Thread(target=worker_thread_func, daemon=True)
    worker.start()
    logging.info("The service is ready")


@app.post("/ocr")
async def create_ocr_task(
    input_s3_path: str = Form(...),
    output_s3_path: Optional[str] = Form(None)
):
    if not input_s3_path:
        raise HTTPException(status_code=400, detail="input_s3_path is empty")
    
    task_id = str(uuid.uuid4())
    event = threading.Event()
    task_events[task_id] = event
    
    task_queue.put((task_id, input_s3_path, output_s3_path))
    
    try:
        await asyncio.to_thread(event.wait, timeout=300.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="OCR task processing timeout")
    finally:
        del task_events[task_id]


    result = task_results.pop(task_id, None)

    if result is None:
        raise HTTPException(status_code=500, detail="results not found")
    
    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=500, detail=result['error'])

    return JSONResponse(
        status_code=200,
        content={
            "message": result
        }
    )

@app.get("/")
def read_root():
    return {"status": "Docling OCR Service is running."}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6008)
