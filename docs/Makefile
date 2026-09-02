# Minimal makefile for Sphinx documentation
# Extended with a `serve` target for local preview.

SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build
SERVEPORT     ?= 8000

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile serve

# Build HTML and open a local HTTP server for live preview.
serve: html
	@echo "Serving documentation at http://localhost:$(SERVEPORT)"
	@cd "$(BUILDDIR)/html" && python3 -m http.server $(SERVEPORT) --bind 127.0.0.1

%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)