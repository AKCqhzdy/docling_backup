import logging
import warnings
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import List, Optional, Type

import numpy as np
from docling_core.types.doc import BoundingBox, CoordOrigin
from docling_core.types.doc.page import BoundingRectangle, TextCell

from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import Page
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import (
    MyOcrOptions,
    OcrOptions,
)
from docling.datamodel.settings import settings
from docling.models.base_ocr_model import BaseOcrModel
from docling.utils.accelerator_utils import decide_device
from docling.utils.profiling import TimeRecorder
from docling.utils.utils import download_url_with_progress

import requests
import base64
from io import BytesIO
from PIL import Image
import re
from paddleocr import LayoutDetection
import json
import concurrent.futures

_log = logging.getLogger(__name__)


class MyOcrModel(BaseOcrModel):
    _model_repo_folder = "MyOcr"

    def __init__(
        self,
        enabled: bool,
        artifacts_path: Optional[Path],
        options: MyOcrOptions,
        accelerator_options: AcceleratorOptions,
    ):
        super().__init__(
            enabled=enabled,
            artifacts_path=artifacts_path,
            options=options,
            accelerator_options=accelerator_options,
        )
        self.options: MyOcrOptions

        self.scale = 3  # multiplier for 72 dpi == 216 dpi.
        self.layout_model = LayoutDetection(model_name="PP-DocLayout_plus-L")

        if self.enabled:
            try:
                import easyocr
            except ImportError:
                raise ImportError(
                    "EasyOCR is not installed. Please install it via `pip install easyocr` to use this OCR engine. "
                    "Alternatively, Docling has support for other OCR engines. See the documentation."
                )

            if self.options.use_gpu is None:
                device = decide_device(accelerator_options.device)
                # Enable easyocr GPU if running on CUDA, MPS
                use_gpu = any(
                    device.startswith(x)
                    for x in [
                        AcceleratorDevice.CUDA.value,
                        AcceleratorDevice.MPS.value,
                    ]
                )
            else:
                warnings.warn(
                    "Deprecated field. Better to set the `accelerator_options.device` in `pipeline_options`. "
                    "When `use_gpu and accelerator_options.device == AcceleratorDevice.CUDA` the GPU is used "
                    "to run EasyOCR. Otherwise, EasyOCR runs in CPU."
                )
                use_gpu = self.options.use_gpu

            download_enabled = self.options.download_enabled
            model_storage_directory = self.options.model_storage_directory
            if artifacts_path is not None and model_storage_directory is None:
                download_enabled = False
                model_storage_directory = str(artifacts_path / self._model_repo_folder)

            self.reader = easyocr.Reader(
                lang_list=self.options.lang,
                gpu=use_gpu,
                model_storage_directory=model_storage_directory,
                recog_network=self.options.recog_network,
                download_enabled=download_enabled,
                verbose=False,
            )

    @staticmethod
    def download_models(
        detection_models: List[str] = ["craft"],
        recognition_models: List[str] = ["english_g2", "latin_g2"],
        local_dir: Optional[Path] = None,
        force: bool = False,
        progress: bool = False,
    ) -> Path:
        # Models are located in https://github.com/JaidedAI/EasyOCR/blob/master/easyocr/config.py
        from easyocr.config import (
            detection_models as det_models_dict,
            recognition_models as rec_models_dict,
        )

        if local_dir is None:
            local_dir = settings.cache_dir / "models" / MyOcrModel._model_repo_folder

        local_dir.mkdir(parents=True, exist_ok=True)

        # Collect models to download
        download_list = []
        for model_name in detection_models:
            if model_name in det_models_dict:
                download_list.append(det_models_dict[model_name])
        for model_name in recognition_models:
            if model_name in rec_models_dict["gen2"]:
                download_list.append(rec_models_dict["gen2"][model_name])

        # Download models
        for model_details in download_list:
            buf = download_url_with_progress(model_details["url"], progress=progress)
            with zipfile.ZipFile(buf, "r") as zip_ref:
                zip_ref.extractall(local_dir)

        return local_dir

    def __call__(
        self, conv_res: ConversionResult, page_batch: Iterable[Page]
    ) -> Iterable[Page]:
        if not self.enabled:
            yield from page_batch
            return

        for page in page_batch:
            assert page._backend is not None
            if not page._backend.is_valid():
                yield page
            else:
                with TimeRecorder(conv_res, "ocr"):
                    ocr_rects = self.get_ocr_rects(page)
                    print(len(ocr_rects))
                    exit(0)
                    # print(ocr_rects)
                    # ocr_rects = self.get_ocr_rects2(page)
                    # print(ocr_rects)
                    # exit(0)

                    def handle_one_ocr_rect(ocr_rect: BoundingBox) -> List[TextCell]:
                        if ocr_rect.area() == 0:
                            return None
                        high_res_image = page._backend.get_page_image(
                            scale=self.scale, cropbox=ocr_rect
                        )

                        # send requset
                        def build_no_anchoring_yaml_prompt() -> str:
                            return (
                                "Attached is one page of a document that you must process. "
                                "Just return the plain text representation of this document as if you were reading it naturally. Convert equations to LateX and tables to markdown.\n"
                                "Return your output as markdown, with a front matter section on top specifying values for the primary_language, is_rotation_valid, rotation_correction, is_table, and is_diagram parameters."
                            )
                        def send_reqeust_to_olmocr(image: Image.Image):
                            # encode to base64
                            buffered = BytesIO()
                            image.save(buffered, format="PNG")
                            image_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
                            prompt = build_no_anchoring_yaml_prompt()

                            headers = {
                                "Content-Type": "application/json",
                            }

                            payload = {
                                "model": "olmOCR-7B",
                                "messages": [
                                    {
                                        "role": "user",
                                        "content": [
                                            {
                                                "type": "image_url",
                                                "image_url": {
                                                    "url": f"data:image/png;base64,{image_base64}"
                                                }
                                            },
                                            {
                                                "type": "text",
                                                "text": prompt
                                            }
                                        ]
                                    }
                                ],
                                "max_tokens": 4096,
                                "temperature": 0.0
                            }

                            response = requests.post("http://olmocr-7b:6008/v1/chat/completions", json=payload, headers=headers)
                            response.raise_for_status()

                            return response.json()


                        response = send_reqeust_to_olmocr(high_res_image)
                        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
                        parsed = json.loads(content)
                        content_text = parsed.get("text", "")
                        # print(content_text)
                        # exit(0)
                        cells = TextCell(
                                index=0,
                                text=content_text,
                                orig=content_text,
                                from_ocr=True,
                                confidence=1,
                                rect=BoundingRectangle.from_bounding_box(ocr_rect)
                            )
                        return cells
                    
                    # all_ocr_cells = []
                    # for ocr_rect in ocr_rects:
                    #     # Skip zero area boxes
                    #     cells = handle_one_ocr_rect(ocr_rect)
                    #     if cells is not None:
                    #         all_ocr_cells.extend(cells)

                    MAX_WORKERS = 4 
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        results_iterator = executor.map(handle_one_ocr_rect, ocr_rects)
                    all_ocr_cells = [result for result in results_iterator if result is not None]
                    
                    self.post_process_cells(all_ocr_cells, page)

                # DEBUG code:
                if settings.debug.visualize_ocr:
                    self.draw_ocr_rects_and_cells(conv_res, page, ocr_rects)

                yield page

    @classmethod
    def get_options_type(cls) -> Type[OcrOptions]:
        return MyOcrOptions


    def get_ocr_rects2(self, page: Page) -> List[BoundingBox]: # use paddleocr to detect text boxes
        assert page.size is not None
        img_np = np.array(page.get_image())
        results = self.layout_model.predict(img_np, batch_size=1, layout_nms=True)

        ocr_boxes = []
        for res in results:
            for box in res['boxes']:
                xmin, ymin, xmax, ymax = map(float, box['coordinate'])
                ocr_boxes.append(
                    BoundingBox(
                        l=xmin,
                        t=ymin,
                        r=xmax,
                        b=ymax,
                        coord_origin=CoordOrigin.TOPLEFT
                    )
                )

        return ocr_boxes
