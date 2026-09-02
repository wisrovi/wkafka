"""WKafka documentation build configuration.

Generated for the WKafka project by Sphinx.

All configuration here expects Sphinx >= 7 and the ``furo`` theme.
"""

import os
import sys

sys.path.insert(0, os.path.abspath("../../"))

project = "WKafka"
copyright = "2026, William R. (wisrovi)"
author = "William R. (wisrovi)"
release = "1.0.6"

github_repo = "wisrovi/wkafka"
github_doc_root = "https://github.com/wisrovi/wkafka/blob/main/docs/source"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "myst_parser",
]

# Napoleon: support for Google / NumPy-style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False

# MyST: allow Markdown content alongside RST
myst_heading_anchors = 3

# Intersphinx: cross-link to the Python standard library docs
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Autodoc: always reference the current classes
autodoc_typehints = "description"
autodoc_member_order = "bysource"

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_title = "WKafka Documentation"
html_theme_options = {
    "light_logo": "wkafka.svg",
    "dark_logo": "wkafka.svg",
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/wisrovi/wkafka",
            "html": (
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">'
                '<path fill="currentColor" d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 '
                "11.385.6.105.825-.255.825-.585 0-.285-.015-1.245-.015-2.265-3.015.585-3.79-1.215-4.03-2.325-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.655-.3-5.43-1.325-5.43-5.895 0-1.305.465-2.37 1.23-3.195-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.825 1.23 1.89 1.23 3.195 0 4.575-2.785 5.595-5.43 5.895.45.39.825 1.14.825 2.295 0 1.665-.015 3.015-.015 3.42 0 .33.225.705.855.585C20.565 21.795 24 17.31 24 11.995 24 5.37 18.63 0 12 0z\"/>"
                "</svg>"
            ),
        }
    ],
    "announcement": (
        '<p>WKafka v1.0.6 &mdash; professional, decorator-based Kafka for Python.</p>'
    ),
}

html_static_path = ["_static"]
html_css_files = ["custom.css"]

# -- Custom HTML overrides ---------------------------------------------------
# Expose author identity as a live link so citation is one click away.
html_meta = {
    "author": "William R. (wisrovi)",
}