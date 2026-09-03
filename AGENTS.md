# AGENTS.md — Rules & Guidelines for WKafka OS

Obligatory rules for any AI agent or developer working on this workspace.

---

## 🚀 1. PyPI Package Deployment Rule (Global for all wisrovi libraries)

- **Standard Publishing Command**: To build and upload Python packages to PyPI across all wisrovi projects, ALWAYS use `twine` with the system `.pypirc` credentials:
  ```bash
  poetry build
  poetry run twine upload dist/*
  ```
- **Reason**: `poetry publish` may fail with `HTTP 403 Forbidden` if API tokens are not explicitly stored in Poetry's internal CLI config. `twine` automatically reads the authenticated PyPI API token from `~/.pypirc`.

---

## 📦 2. Git & Commit Guidelines

- **One File per Commit**: Every modified or added file must be committed individually (`1 file = 1 commit`).
- **Commit Message Format**: Strict conventional brackets in perfect English:
  - `[FEATURE]` for new capabilities.
  - `[FIX]` for bug fixes.
  - `[REFACTOR]` for code restructuring or dependency updates.
  - `[DOCS]` for documentation updates.
  - `[STYLE]` for code formatting (e.g. Black, Ruff).
  - `[TEST]` for test suite additions/updates.
  - `[SECURITY]` for secret baselines or security settings.
  - `[RELEASE]` for version releases.

---

## 🧪 3. Unit Testing & Coverage

- **Framework**: `pytest`.
- **Coverage Script**: `./run_coverage.sh` (guarantees HTML report and minimum 80% coverage target).
- **Docker Isolated Testing**: All unit tests MUST pass inside Docker container before completion:
  ```bash
  ./run_tests_docker.sh
  ```
- **Documentation**: All test files must contain explanatory docstrings and tests must be documented in `README.md`.

---

## 🔐 4. Pre-commit & Secrets Baseline

- All edited files MUST pass pre-commit checks (`poetry run pre-commit run --files <edited_files>`).
- Repositories with `.pre-commit-config.yaml` MUST contain a `.secrets.baseline` file.

---

## 📖 5. Documentation & README

- Every repository MUST maintain a synchronized `README.md`.
- `README.md` must include a **"Tecnologías y Librerías Relevantes"** section and a **"Unit Testing & Coverage"** section.
