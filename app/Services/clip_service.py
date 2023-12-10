import numpy.random
import torch
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from torch import FloatTensor, no_grad
from app.config import config
from loguru import logger
from time import time
from numpy import ndarray


class ClipService:
    def __init__(self):
        self.device = config.clip.device
        if self.device == "auto":
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info("Using device: {}; Model: {}", self.device, config.clip.model)
        self.model = CLIPModel.from_pretrained(config.clip.model).to(self.device)
        self.processor = CLIPProcessor.from_pretrained(config.clip.model)
        logger.success("Model loaded successfully")

    @no_grad()
    def get_image_vector(self, image: Image.Image) -> ndarray:
        if image.mode != "RGB":
            image = image.convert("RGB")
        logger.info("Processing image...")
        start_time = time()
        inputs = self.processor(images=image, return_tensors="pt").to(self.device)
        logger.success("Image processed, now inferencing with CLIP model...")
        outputs: FloatTensor = self.model.get_image_features(**inputs)
        logger.success("Inference done. Time elapsed: {:.2f}s", time() - start_time)
        logger.info("Norm: {}", outputs.norm(dim=-1).item())
        outputs /= outputs.norm(dim=-1, keepdim=True)
        return outputs.numpy(force=True).reshape(-1)

    @no_grad()
    def get_text_vector(self, text: str) -> ndarray:
        logger.info("Processing text...")
        start_time = time()
        inputs = self.processor(text=text, return_tensors="pt").to(self.device)
        logger.success("Text processed, now inferencing with CLIP model...")
        outputs: FloatTensor = self.model.get_text_features(**inputs)
        logger.success("Inference done. Time elapsed: {:.2f}s", time() - start_time)
        logger.info("Norm: {}", outputs.norm(dim=-1).item())
        outputs /= outputs.norm(dim=-1, keepdim=True)
        return outputs.numpy(force=True).reshape(-1)

    @staticmethod
    def get_random_vector() -> ndarray:
        vec = numpy.random.rand(768)
        vec -= vec.mean()
        return vec


