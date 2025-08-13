import os
import threading
import time
import json
from pathlib import Path
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Form, HTTPException, Response
from starlette.responses import JSONResponse
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    MyOcrOptions,
)
from typing import Optional, Tuple, Literal
from docling.document_converter import DocumentConverter, PdfFormatOption
import logging
import boto3
import codecs
from botocore.config import Config
import httpx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
s3_client = boto3.client('s3')

endpoint = os.getenv('OSS_ENDPOINT')
access_key_id = os.getenv('OSS_ACCESS_KEY_ID')
secret_access_key = os.getenv('OSS_ACCESS_KEY_SECRET')
bucket_name = os.getenv('OSS_BUCKET_NAME')

s3_oss_client = boto3.client(
    's3',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    endpoint_url="https://oss-cn-hongkong.aliyuncs.com",
    config=Config(s3={"addressing_style": "virtual"},
                  signature_version='s3'))


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)


GLOBAL_LOCK_MANAGER = threading.Lock() 
PROCESSING_INPUT_LOCKS = {} 
PROCESSING_OUTPUT_LOCKS = {} 

docling_converter: Optional[DocumentConverter] = None
executor = ThreadPoolExecutor(max_workers=4) 

def perform_ocr(input_s3_path: str, output_s3_path: str):
    if docling_converter is None:
        raise RuntimeError("DocumentConverter has not been initialized.")

    logging.info(f"start to deal with: {input_s3_path}")

    is_s3 = False
    if input_s3_path.startswith("s3://") and output_s3_path.startswith("s3://"):
        is_s3 = True
    elif input_s3_path.startswith("oss://") and input_s3_path.startswith("oss://"):
        is_s3 = False
    else:
        raise RuntimeError(f"must use s3 or oss. {str(e)}") from e
    
    def parse_s3_path(s3_path: str):
        if is_s3:
            s3_path = s3_path.replace("s3://", "")
        else:
            s3_path = s3_path.replace("oss://", "")
        bucket, *key_parts = s3_path.split("/")
        return bucket, "/".join(key_parts)

    file_bucket, file_key = parse_s3_path(input_s3_path)
    input_file_path = INPUT_DIR / file_bucket / file_key
    input_file_path.parent.mkdir(parents=True, exist_ok=True)


    
    with GLOBAL_LOCK_MANAGER:
        if input_s3_path not in PROCESSING_INPUT_LOCKS:
            PROCESSING_INPUT_LOCKS[input_s3_path] = threading.Lock()
        if output_s3_path not in PROCESSING_OUTPUT_LOCKS:
            PROCESSING_OUTPUT_LOCKS[output_s3_path] = threading.Lock()
    input_lock = PROCESSING_INPUT_LOCKS[input_s3_path]
    output_lock = PROCESSING_OUTPUT_LOCKS[output_s3_path]
    
    with input_lock:
        with output_lock:

        # download file from S3
            try:
                if is_s3:
                    s3_client.download_file(
                        Bucket=file_bucket,
                        Key=file_key,
                        Filename=str(input_file_path)
                    )
                else:
                    s3_oss_client.download_file(
                        Bucket=file_bucket,
                        Key=file_key,
                        Filename=str(input_file_path)
                    )
                logging.info(f"download from s3/oss successfully: {input_s3_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to download file from S3/oss: {str(e)}") from e


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
                if is_s3:
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
                else:
                    s3_oss_client.upload_file(
                        Bucket=output_bucket,
                        Key=f"{output_key}.md",
                        Filename=str(output_md_path)
                    )
                    s3_oss_client.upload_file(
                        Bucket=output_bucket,
                        Key=f"{output_key}.json",
                        Filename=str(output_json_path)
                    )
                logging.info(f"upload from s3/oss successfully: {output_file_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to upload file from S3/oss: {str(e)}") from e
        
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


TARGET_URL = "http://olmocr-7b:6008/health"
async def health_check():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(TARGET_URL, timeout=5.0)

        headers_to_exclude = {
            "content-encoding",
            "content-length",
            "transfer-encoding",
            "connection",
        }
        proxied_headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in headers_to_exclude
        }

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=proxied_headers,
            media_type=response.headers.get("content-type")
        )
    except httpx.ConnectError as e:
        return JSONResponse(
            status_code=503,
            content={"success": False, "status": 503, "detail": f"Health check failed: Unable to connect to DotsOCR service. Error: {e}"}
        )
    except httpx.TimeoutException as e:
        return JSONResponse(
            status_code=504,
            content={"success": False, "status": 504,"detail": f"Health check failed: Request to DotsOCR service timed out. Error: {e}"}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "status": 500,"detail": f"An unexpected error occurred during health check. Error: {e}"}
        )
    

@app.get("/health")
async def health():
    return await health_check()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6008)
