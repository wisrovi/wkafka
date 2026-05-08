import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import numpy as np
import yaml
import cv2
from PIL import Image

class Serializer(ABC):
    @abstractmethod
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        pass

class JSONSerializer(Serializer):
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        return json.dumps(value).encode("utf-8")

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        return json.loads(data.decode("utf-8"))

class YAMLSerializer(Serializer):
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        return yaml.safe_dump(value).encode("utf-8")

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        return yaml.safe_load(data.decode("utf-8"))

class ImageSerializer(Serializer):
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        quality = kwargs.get("quality", 80)
        if isinstance(value, Image.Image):
            value = np.array(value)
        
        if not isinstance(value, np.ndarray):
            raise TypeError(f"Expected numpy.ndarray or PIL.Image, got {type(value)}")
            
        success, encoded_image = cv2.imencode(".jpg", value, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
        if not success:
            raise ValueError("Failed to encode image")
        return encoded_image.tobytes()

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        np_arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("Failed to decode image")
        return img
