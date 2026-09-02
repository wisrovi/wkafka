# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-05-08

### Added
- **Core Refactoring**: Completely redesigned architecture under `wkafka.core`.
- **LTS Support**: Declared version 1.0.0 as Long Term Support (LTS).
- **Python Compatibility**: Official support for Python 3.9, 3.10, 3.11, 3.12, 3.13, and 3.14.
- **SASL Authentication**: Native support for SASL_PLAINTEXT (PLAIN and SCRAM) mechanisms.
- **KRaft Mode Support**: Environment updated to support Kafka without Zookeeper.
- **Type Hinting**: Added PEP 484 type hints across the entire codebase.
- **Modern Packaging**: Migrated dependency management and packaging to **Poetry**.
- **Backward Compatibility**: Added a controller bridge to support legacy code and existing examples.
- **Structured Logging**: Deep integration with `loguru` for professional-grade logging.
- **Enhanced Serializers**: Dedicated serialization layer for JSON, YAML, and Images (NumPy/PIL).
- **Multi-threading**: Improved consumer execution using managed thread pools.
- **CI/CD Ready**: Configured `tox` for automated multi-version testing.

### Changed
- Improved `Wkafka` class to be an alias of the new `WKafka` orchestrator.
- Updated `docker-compose` files to use modern Kafka images (Bitnami/Apache) and KRaft mode.
- Refactored `send` method to handle headers and binary data more robustly.

### Removed
- Removed legacy `installer.sh` and other shell scripts.
- Removed unused and unmaintained dependencies.
- Cleaned up root directory from temporary and junk files.

---
*Initial "Professional" Release (Major Refactor)*
