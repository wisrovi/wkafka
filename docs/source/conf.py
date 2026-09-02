import os
import sys

sys.path.insert(0, os.path.abspath("../../"))

project = "WKafka"
copyright = "2026, WILLIAM R. (wisrovi)"
author = "WILLIAM R. (wisrovi)"
release = "1.0.6"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
