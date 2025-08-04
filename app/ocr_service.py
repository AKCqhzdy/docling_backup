import os
import threading
import time
import json
from pathlib import Path
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, HTTPException
from starlette.responses import JSONResponse
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    MyOcrOptions,
)
from typing import Optional
from docling.document_converter import DocumentConverter, PdfFormatOption
import logging
import boto3
import codecs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
s3_client = boto3.client('s3')

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)

docling_converter: Optional[DocumentConverter] = None
executor = ThreadPoolExecutor(max_workers=4) 

def perform_ocr(input_s3_path: str, output_s3_path: str):
    if docling_converter is None:
        raise RuntimeError("DocumentConverter has not been initialized.")

    logging.info(f"start to deal with: {input_s3_path}")
    
    def parse_s3_path(s3_path: str):
        s3_path = s3_path.replace("s3://", "")
        bucket, *key_parts = s3_path.split("/")
        return bucket, "/".join(key_parts)

    file_bucket, file_key = parse_s3_path(input_s3_path)
    input_file_path = INPUT_DIR / file_bucket / file_key
    input_file_path.parent.mkdir(parents=True, exist_ok=True)

    # download file from S3
    try:
        s3_client.download_file(
            Bucket=file_bucket,
            Key=file_key,
            Filename=str(input_file_path)
        )
        logging.info(f"download from s3 successfully: {input_s3_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to download file from S3: {str(e)}") from e


    output_bucket, output_key = parse_s3_path(output_s3_path)
    output_key = output_key + "/" + output_s3_path.rstrip("/").split("/")[-1]
    output_file_path = OUTPUT_DIR / output_bucket / output_key
    output_md_path = output_file_path.with_suffix(".md")
    output_json_path = output_file_path.with_suffix(".json")
    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    # convert the file
    try:
        doc = docling_converter.convert(str(input_file_path)).document
        md_content = doc.export_to_markdown()
        with open(output_md_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(doc.export_to_dict(), f, ensure_ascii=False, indent=4)

        logging.info(f"task finish: {input_s3_path}")

    except Exception as e:
        raise RuntimeError(f"An error occurred when processing the file {input_s3_path}: {e}") from e
    
    # upload the file to S3
    try:
        s3_client.upload_file(
            Bucket=output_bucket,
            Key=f"{output_key}.md",
            Filename=str(output_md_path)
        )
        s3_client.upload_file(
            Bucket=output_bucket,
            Key=f"{output_key}.json",
            Filename=str(output_json_path)
        )
        logging.info(f"upload from s3 successfully: {output_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload file from S3: {str(e)}") from e
    
    # finally:
    #     try:
    #         os.remove(input_file_path)
    #         if 'output_md_path' in locals() and os.path.exists(output_md_path):
    #             os.remove(output_md_path)
    #         if 'output_json_path' in locals() and os.path.exists(output_json_path):
    #             os.remove(output_json_path)
    #     except OSError as e:
    #         logging.warning(f"Failed to clear the temporary files: {e}")
    return md_content




@asynccontextmanager
async def lifespan(app: FastAPI):
    global docling_converter
    logging.info("Initializing DocumentConverter...")
    loop = asyncio.get_event_loop()
    docling_converter = await loop.run_in_executor(executor, initialize_converter)
    logging.info("DocumentConverter Initialized.")
    logging.info(f"The service is ready. The number of working threads is {executor._max_workers}")
    
    yield

    executor.shutdown(wait=True)
    logging.info("service has been shutdown.")

def initialize_converter():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(INPUT_DIR, exist_ok=True)
    
    pipeline_options = PdfPipelineOptions()
    # ocr_options = EasyOcrOptions(force_full_page_ocr=True)
    ocr_options = MyOcrOptions(force_full_page_ocr=True)
    pipeline_options.ocr_options = ocr_options
    
    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )

app = FastAPI(title="Docling OCR Service", lifespan=lifespan)


@app.post("/ocr")
async def create_ocr_task(
    input_s3_path: str = Form(...),
    output_s3_path: Optional[str] = Form(None)
):
    if not input_s3_path:
        raise HTTPException(status_code=400, detail="input_s3_path is empty")

        
    loop = asyncio.get_event_loop()
    
    try:
        logging.info(f"The task has been submitted to the thread pool: {input_s3_path}")
        result = await loop.run_in_executor(
            executor, perform_ocr, input_s3_path, output_s3_path
        )
        return JSONResponse(
            status_code=200,
            content={
                "message": "OCR task completed successfully.",
                "markdown_content": result[:50],
                "output_s3_path": output_s3_path
            }
        )
    except Exception as e:
        logging.error(f"Task processing failed {input_s3_path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"status": "Docling OCR Service is running."}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6008)
