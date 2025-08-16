from pathlib import Path
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    MyOcrOptions
)
from docling.document_converter import DocumentConverter, PdfFormatOption
import json

def main():
    pipeline_options = PdfPipelineOptions()

    op = 0
    if op == 0:
        ocr_options = MyOcrOptions(force_full_page_ocr=True)
        file_name = "test"
    else:
        ocr_options = EasyOcrOptions(force_full_page_ocr=True)
        file_name = "easyocr"
    # 
    # Any of the OCR options can be used:EasyOcrOptions, TesseractOcrOptions, TesseractCliOcrOptions, OcrMacOptions(Mac only), RapidOcrOptions
    # ocr_options = EasyOcrOptions(force_full_page_ocr=True)
    # ocr_options = TesseractOcrOptions(force_full_page_ocr=True)
    # ocr_options = OcrMacOptions(force_full_page_ocr=True)
    # ocr_options = RapidOcrOptions(force_full_page_ocr=True)
    # ocr_options = TesseractCliOcrOptions(force_full_page_ocr=True)
    pipeline_options.ocr_options = ocr_options

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )

    prefix = "/usr/local/lib/python3.10/dist-packages/docling/yzhtest"
    input_doc_path = f"{prefix}/test.pdf"
    doc = converter.convert(input_doc_path).document
    md = doc.export_to_markdown()
    suffix = "_table"
    output_file_path = f"{prefix}/output/{file_name}{suffix}.md"
    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write(md)
    output_file_path_json = f"{prefix}/output/{file_name}{suffix}.json"
    with open(output_file_path_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(doc.export_to_dict()))

if __name__ == "__main__":
    main()
