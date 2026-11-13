"""Comprehensive unit tests for WKafka offload and YAML configuration parsing.

Validates:
1. Option A: Loading full configuration from a YAML file (including 'kafka:' section and 'offload:').
2. Option B: Loading dedicated offload configuration from a standalone YAML file path.
3. Option C: In-memory dictionary configuration (both minimal and extended formats).
4. Option D: Automatic fallback to environment variables (KAFKA_CONFIG, REDIS_HOST, REDIS_PORT).
5. Inactive offload: Default state when offload is None or not present.
"""

import os
import tempfile
import pytest

from wkafka.core.config import OffloadConfig, parse_offload_config, resolve_kafka_config


class TestOffloadConfigParsing:
    """Test suite validating all configuration modes for offload and YAML settings."""

    def test_default_inactive_offload(self):
        """Validates that by default offload is disabled and inactive when no configuration is passed."""
        cfg = parse_offload_config(None)
        assert isinstance(cfg, OffloadConfig)
        assert cfg.enabled is False

    def test_option_c_minimal_dict(self):
        """Validates Option C with minimal dictionary: {'backend': 'redis', 'ttl': 5}.

        Ensures default host/port and image/file formats are populated automatically.
        """
        cfg = parse_offload_config({"backend": "redis", "ttl": 5})
        assert cfg.enabled is True
        assert cfg.backend == "redis"
        assert cfg.ttl == 5
        assert cfg.host == "localhost"
        assert cfg.port == 6379
        assert cfg.on_expired == "drop"
        assert "image" in cfg.formats
        assert "file" in cfg.formats

    def test_option_c_extended_dict(self):
        """Validates Option C with extended parameters: custom IP, port, password, threshold, and key prefix."""
        cfg = parse_offload_config(
            {
                "backend": "redis",
                "host": "192.168.1.68",
                "port": 15963,
                "password": "secret_password",
                "db": 2,
                "ttl": 10,
                "on_expired": "none",
                "formats": ["image"],
                "threshold_kb": 64,
                "key_prefix": "custom:claim",
            }
        )
        assert cfg.enabled is True
        assert cfg.host == "192.168.1.68"
        assert cfg.port == 15963
        assert cfg.password == "secret_password"
        assert cfg.db == 2
        assert cfg.ttl == 10
        assert cfg.on_expired == "none"
        assert cfg.formats == ["image"]
        assert cfg.threshold_kb == 64
        assert cfg.key_prefix == "custom:claim"

    def test_option_b_dedicated_offload_yaml(self):
        """Validates Option B where offload argument is a path to a dedicated YAML file."""
        yaml_content = """
        backend: redis
        host: 192.168.1.68
        port: 15963
        ttl: 7
        on_expired: drop
        """
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            cfg = parse_offload_config(yaml_path)
            assert cfg.enabled is True
            assert cfg.host == "192.168.1.68"
            assert cfg.port == 15963
            assert cfg.ttl == 7
            assert cfg.on_expired == "drop"
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)

    def test_option_a_full_yaml_config(self):
        """Validates Option A where a comprehensive application YAML is provided with a 'kafka:' section."""
        yaml_content = """
        kafka:
          bootstrap_servers: "remote-broker:9092"
          client_id: "extractor-01"
          offload:
            backend: "redis"
            host: "10.0.0.5"
            port: 6379
            ttl: 3
        """
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            resolved = resolve_kafka_config(config_path=yaml_path)
            assert resolved["bootstrap_servers"] == "remote-broker:9092"
            assert resolved["client_id"] == "extractor-01"

            cfg = parse_offload_config(resolved.get("offload"))
            assert cfg.enabled is True
            assert cfg.host == "10.0.0.5"
            assert cfg.ttl == 3
        finally:
            if os.path.exists(yaml_path):
                os.remove(yaml_path)

    def test_option_d_environment_variables_fallback(self, monkeypatch):
        """Validates Option D where environment variables supply host, port, and password."""
        monkeypatch.setenv("REDIS_HOST", "env-redis-host")
        monkeypatch.setenv("REDIS_PORT", "16379")
        monkeypatch.setenv("REDIS_PASSWORD", "env-password")

        cfg = parse_offload_config({"backend": "redis", "ttl": 5})
        assert cfg.enabled is True
        assert cfg.host == "env-redis-host"
        assert cfg.port == 16379
        assert cfg.password == "env-password"
