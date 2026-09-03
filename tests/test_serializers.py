"""
Unit tests for WKafka Serializers.

This module tests all built-in serializers: JSONSerializer, YAMLSerializer,
ImageSerializer, PydanticSerializer, and FileSerializer.
Each test case verifies both serialization to bytes and deserialization back to native types.
"""

import numpy as np
from pydantic import BaseModel
from PIL import Image
from wkafka.serializers.base import (
    FileSerializer,
    ImageSerializer,
    JSONSerializer,
    PydanticSerializer,
    YAMLSerializer,
)


def test_json_serializer():
    """
    Validates JSONSerializer encoding and decoding.
    Verifies that native Python dict structures are properly converted to UTF-8 bytes
    and deserialized back to an identical dictionary.
    """
    serializer = JSONSerializer()
    payload = {"id": 123, "name": "Kafka Test"}

    encoded = serializer.serialize(payload)
    assert isinstance(encoded, bytes)

    decoded = serializer.deserialize(encoded)
    assert decoded == payload


def test_yaml_serializer():
    """
    Validates YAMLSerializer encoding and decoding.
    Verifies that dictionary data is serialized to valid YAML byte streams
    and correctly reconstructed upon deserialization.
    """
    serializer = YAMLSerializer()
    payload = {"service": "worker", "replicas": 3}

    encoded = serializer.serialize(payload)
    assert isinstance(encoded, bytes)

    decoded = serializer.deserialize(encoded)
    assert decoded == payload


def test_image_serializer_numpy():
    """
    Validates ImageSerializer using NumPy arrays (OpenCV BGR format).
    Verifies that a 3D NumPy array representing a frame is compressed to JPEG bytes
    and deserialized into a valid NumPy array with matching dimensions.
    """
    serializer = ImageSerializer()
    dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)

    encoded = serializer.serialize(dummy_frame, quality=80)
    assert isinstance(encoded, bytes)

    decoded = serializer.deserialize(encoded)
    assert isinstance(decoded, np.ndarray)
    assert decoded.shape == dummy_frame.shape


def test_image_serializer_pil():
    """
    Validates ImageSerializer using PIL Image objects.
    Verifies that a PIL RGB Image is converted and serialized to JPEG bytes without error.
    """
    serializer = ImageSerializer()
    pil_img = Image.new("RGB", (50, 50), color="red")

    encoded = serializer.serialize(pil_img)
    assert isinstance(encoded, bytes)

    decoded = serializer.deserialize(encoded)
    assert isinstance(decoded, np.ndarray)
    assert decoded.shape == (50, 50, 3)


class UserSchema(BaseModel):
    user_id: int
    username: str


def test_pydantic_serializer():
    """
    Validates PydanticSerializer schema validation and serialization.
    Verifies that a Pydantic model instance is serialized to JSON bytes,
    and when deserialized with the model parameter, returns a validated model instance.
    """
    serializer = PydanticSerializer()
    user = UserSchema(user_id=42, username="alice")

    encoded = serializer.serialize(user)
    assert isinstance(encoded, bytes)

    decoded = serializer.deserialize(encoded, model=UserSchema)
    assert isinstance(decoded, UserSchema)
    assert decoded.user_id == 42
    assert decoded.username == "alice"


def test_file_serializer(tmp_path):
    """
    Validates FileSerializer handling raw bytes and file paths.
    Verifies that raw bytes, file paths, dicts, or strings are serialized correctly.
    """
    serializer = FileSerializer()
    test_bytes = b"Sample raw binary file data"

    encoded = serializer.serialize(test_bytes)
    assert encoded == test_bytes

    decoded = serializer.deserialize(encoded)
    assert decoded == test_bytes

    # Test file path on disk
    file_path = tmp_path / "test.txt"
    file_path.write_bytes(b"File content from disk")
    encoded_file = serializer.serialize(str(file_path))
    assert encoded_file == b"File content from disk"

    # Test dict with bytes content
    encoded_dict_bytes = serializer.serialize({"content": b"dict bytes"})
    assert encoded_dict_bytes == b"dict bytes"

    # Test dict with string content
    encoded_dict_str = serializer.serialize({"content": "dict str"})
    assert encoded_dict_str == b"dict str"

    # Test plain string
    encoded_str = serializer.serialize("plain string content")
    assert encoded_str == b"plain string content"

    import pytest

    with pytest.raises(TypeError):
        serializer.serialize(12345)


def test_image_serializer_errors():
    """
    Validates ImageSerializer exception branches for invalid input types and decoding failures.
    """
    import pytest

    serializer = ImageSerializer()

    # Invalid input type
    with pytest.raises(TypeError):
        serializer.serialize("not_an_image")

    # Invalid image bytes
    with pytest.raises(ValueError):
        serializer.deserialize(b"invalid_jpeg_data")


def test_pydantic_serializer_fallbacks():
    """
    Validates PydanticSerializer fallbacks for dicts, primitive strings, and legacy model methods.
    """
    serializer = PydanticSerializer()

    # Legacy model with .json() method
    class LegacyModel:
        def json(self):
            return '{"legacy": true}'

    encoded_legacy = serializer.serialize(LegacyModel())
    assert encoded_legacy == b'{"legacy": true}'

    # Dict input
    encoded_dict = serializer.serialize({"dict_key": "value"})
    assert encoded_dict == b'{"dict_key": "value"}'

    # Primitive string fallback
    encoded_str = serializer.serialize(12345)
    assert encoded_str == b"12345"

    # Deserialization without model
    decoded_raw = serializer.deserialize(b'{"key": "val"}')
    assert decoded_raw == {"key": "val"}

    # Deserialization with legacy parse_raw model
    class LegacyParseModel:
        @classmethod
        def parse_raw(cls, text):
            return {"parsed": text}

    decoded_legacy_parse = serializer.deserialize(
        b'{"key": "val"}', model=LegacyParseModel
    )
    assert decoded_legacy_parse == {"parsed": '{"key": "val"}'}
