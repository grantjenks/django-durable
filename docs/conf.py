"""Sphinx configuration for django-durable (Markdown via MyST)."""

import os
import sys

# Add project root to sys.path
sys.path.insert(0, os.path.abspath('..'))

project = 'Django Durable'
author = 'Grant Jenks'

extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.doctest',
]

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# MyST configuration
myst_enable_extensions = [
    'colon_fence',  # ```{directive} ... ```
]
myst_heading_anchors = 3

templates_path = ['_templates']
exclude_patterns: list[str] = []

html_theme = 'alabaster'
html_static_path = ['_static']
html_logo = '_static/django-durable-logo.svg'

# Autodoc defaults
autodoc_default_options = {
    'members': True,
    'undoc-members': False,
    'inherited-members': False,
}

# Configure Django so autodoc can import models and modules safely
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'testproj.settings')
try:
    import django  # type: ignore

    django.setup()
except Exception:
    # Allow building API pages that don't need Django when settings are unavailable
    pass
