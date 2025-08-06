from pathlib import Path
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    EasyOcrOptions,
    MyOcrOptions
)
from docling.document_converter import DocumentConverter, PdfFormatOption
import json
from tqdm import tqdm

def process_single_pdf(converter: DocumentConverter, input_doc_path: Path, output_base_path: Path):
    """
    Processes a single PDF file and saves the output as .md and .json files.

    Args:
        converter: The initialized DocumentConverter instance.
        input_doc_path: The Path object for the input PDF file.
        output_base_path: The Path object for the output file without the extension.
    """
    print(f"-> Processing: {input_doc_path}")
    try:
        # Ensure the parent directory exists
        output_base_path.parent.mkdir(parents=True, exist_ok=True)

        # 1. Convert the document
        doc = converter.convert(str(input_doc_path)).document

        # 2. Export to Markdown and save
        md_content = doc.export_to_markdown()
        md_output_path = Path(f"{str(output_base_path)}.md")
        with open(md_output_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"   Saved Markdown to: {md_output_path}")

        # # 3. Export to JSON and save
        # json_content = doc.export_to_dict()
        # json_output_path = output_base_path.with_suffix(".json")
        # with open(json_output_path, "w", encoding="utf-8") as f:
        #     # Use json.dump for better formatting and handling
        #     json.dump(json_content, f, indent=4)
        # print(f"   Saved JSON to:     {json_output_path}")

    except Exception as e:
        print(f"!!! FAILED to process {input_doc_path}: {e}")


def main():
    # --- Configuration ---
    # Set your main input and output directories
    input_base_dir = Path("/olmOCR-bench/bench_data/pdfs")
    output_base_dir = Path("/olmOCR-bench/bench_data/myocr")

    # Choose your OCR engine (0 for MyOcr, 1 for EasyOcr, etc.)
    op = 0
    if op == 0:
        ocr_options = MyOcrOptions(force_full_page_ocr=True)
        print("Using MyOcrEngine...")
    else:
        ocr_options = EasyOcrOptions(force_full_page_ocr=True)
        print("Using EasyOcrEngine...")
    
    # --- Initialize Converter (do this only once) ---
    pipeline_options = PdfPipelineOptions()
    pipeline_options.ocr_options = ocr_options

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )

    # --- Find and Process all PDF files ---
    print(f"Starting batch processing...")
    print(f"Input directory:  {input_base_dir}")
    print(f"Output directory: {output_base_dir}")

    # Use rglob to find all .pdf files recursively
    pdf_files = list(input_base_dir.rglob("*.pdf"))
    
    if not pdf_files:
        print("No PDF files found in the input directory. Exiting.")
        return

    print(f"Found {len(pdf_files)} PDF files to process.")

    for input_pdf_path in tqdm(pdf_files, desc="Processing PDFs"):
        # This is the key part: create the corresponding output path structure
        # 1. Get the path relative to the input base directory
        #    e.g., "tables/some_document.pdf"
        relative_path = input_pdf_path.relative_to(input_base_dir)

        # 2. Join it with the output base directory
        #    e.g., "/olmOCR-bench/bench_data/myocr/tables/some_document.pdf"
        output_pdf_path = output_base_dir / relative_path

        # 3. Get the path without the file extension ('.pdf')
        #    e.g., "/olmOCR-bench/bench_data/myocr/tables/some_document"
        output_base_path = output_pdf_path.parent / output_pdf_path.stem
        # print(output_pdf_path.parent)
        # print(output_base_path)
        
        # Process the file
        process_single_pdf(converter, input_pdf_path, output_base_path)

    print("\nBatch processing complete.")


if __name__ == "__main__":
    main()