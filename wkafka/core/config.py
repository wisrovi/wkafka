"""Configuration loader and schema definition for WKafka, including payload offloading."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import yaml


@dataclass
class OffloadConfig:
    """Configuration settings for high-throughput payload offloading (Claim-Check pattern)."""

    enabled: bool = False
    backend: str = "redis"
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    ttl: int = 5
    on_expired: str = "drop"  # "drop" or "none"
    formats: List[str] = field(default_factory=lambda: ["image", "file"])
    threshold_kb: int = 0
    key_prefix: str = "wkafka:claim"


def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """Loads and parses a YAML configuration file.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        Dict[str, Any]: Parsed configuration dictionary.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}


def resolve_kafka_config(
    config_path: Optional[str] = None, **kwargs: Any
) -> Dict[str, Any]:
    """Resolves Kafka parameters from a YAML file (if provided) and merges with kwargs.

    Supports Option A (full configuration YAML with optional 'kafka:' section) and
    Option D (KAFKA_CONFIG environment variable).

    Args:
        config_path (Optional[str]): Path to the YAML config file.
        **kwargs: Overriding keyword arguments.

    Returns:
        Dict[str, Any]: Merged configuration dictionary.
    """
    file_path = config_path or os.environ.get("KAFKA_CONFIG")
    resolved: Dict[str, Any] = {}

    if (
        file_path
        and isinstance(file_path, str)
        and (
            file_path.endswith(".yaml")
            or file_path.endswith(".yml")
            or os.path.exists(file_path)
        )
    ):
        data = load_yaml_file(file_path)
        if "kafka" in data and isinstance(data["kafka"], dict):
            resolved.update(data["kafka"])
        else:
            resolved.update(data)

    resolved.update(kwargs)
    return resolved


def parse_offload_config(
    offload_arg: Optional[Union[Dict[str, Any], str, bool]] = None,
    config_path: Optional[str] = None,
) -> OffloadConfig:
    """Parses offload configuration supporting in-memory dict, YAML file path, or boolean flag.

    Args:
        offload_arg (Optional[Union[Dict[str, Any], str, bool]]): Offload config dict, path, or flag.
        config_path (Optional[str]): Optional path to configuration file.

    Returns:
        OffloadConfig: Populated OffloadConfig instance.
    """
    if not offload_arg and not config_path:
        return OffloadConfig(enabled=False)

    data: Dict[str, Any] = {}

    if isinstance(offload_arg, str):
        if (
            offload_arg.endswith(".yaml")
            or offload_arg.endswith(".yml")
            or os.path.exists(offload_arg)
        ):
            # Option B: Dedicated YAML file
            data = load_yaml_file(offload_arg)
        elif offload_arg.lower() in ("redis", "true"):
            data = {"backend": "redis"}
    elif isinstance(offload_arg, dict):
        # Option C: In-memory dictionary
        data = dict(offload_arg)
    elif offload_arg is True:
        data = {"backend": "redis"}
    elif config_path:
        # Check if config_path has an offload section
        file_cfg = resolve_kafka_config(config_path)
        if "offload" in file_cfg and isinstance(file_cfg["offload"], dict):
            data = dict(file_cfg["offload"])

    if not data:
        return OffloadConfig(enabled=False)

    # Defaults and environment overrides (Option D)
    host = data.get("host") or os.environ.get("REDIS_HOST", "localhost")
    port = int(data.get("port") or os.environ.get("REDIS_PORT", 6379))
    password = data.get("password") or os.environ.get("REDIS_PASSWORD", None)
    db = int(data.get("db", 0))
    ttl = int(data.get("ttl", 5))
    backend = str(data.get("backend", "redis")).lower()
    on_expired = str(data.get("on_expired", "drop")).lower()
    formats = data.get("formats", ["image", "file"])
    threshold_kb = int(data.get("threshold_kb", 0))
    key_prefix = str(data.get("key_prefix", "wkafka:claim"))

    return OffloadConfig(
        enabled=True,
        backend=backend,
        host=host,
        port=port,
        password=password,
        db=db,
        ttl=ttl,
        on_expired=on_expired,
        formats=list(formats),
        threshold_kb=threshold_kb,
        key_prefix=key_prefix,
    )
