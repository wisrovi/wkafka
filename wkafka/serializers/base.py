import json
import os
from abc import ABC, abstractmethod
from typing import Any

import cv2
import numpy as np
import yaml
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

        success, encoded_image = cv2.imencode(
            ".jpg", value, [int(cv2.IMWRITE_JPEG_QUALITY), quality]
        )
        if not success:
            raise ValueError("Failed to encode image")
        return encoded_image.tobytes()

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        np_arr = np.frombuffer(data, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError("Failed to decode image")
        return img


class PydanticSerializer(Serializer):
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        if hasattr(value, "model_dump_json"):
            return value.model_dump_json().encode("utf-8")
        if hasattr(value, "json") and callable(value.json):
            return value.json().encode("utf-8")
        if isinstance(value, dict):
            return json.dumps(value).encode("utf-8")
        return str(value).encode("utf-8")

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        model = kwargs.get("model")
        text = data.decode("utf-8")
        if model:
            if hasattr(model, "model_validate_json"):
                return model.model_validate_json(text)
            if hasattr(model, "parse_raw"):
                return model.parse_raw(text)
        return json.loads(text)


class FileSerializer(Serializer):
    def serialize(self, value: Any, **kwargs: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str) and os.path.exists(value):
            with open(value, "rb") as f:
                return f.read()
        if isinstance(value, dict) and "content" in value:
            content = value["content"]
            return content if isinstance(content, bytes) else str(content).encode("utf-8")
        if isinstance(value, str):
            return value.encode("utf-8")
        raise TypeError(f"Expected file path, bytes or dict content, got {type(value)}")

    def deserialize(self, data: bytes, **kwargs: Any) -> Any:
        return data
