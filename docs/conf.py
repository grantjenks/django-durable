"""Sphinx configuration for django-durable."""

import os
import sys

# Add project root to sys.path
sys.path.insert(0, os.path.abspath('..'))

project = 'django-durable'
author = 'django-durable'

extensions = ['myst_parser']

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

templates_path = ['_templates']
exclude_patterns: list[str] = []

html_theme = 'alabaster'
