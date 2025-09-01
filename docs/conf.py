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
html_css_files = ['custom.css']

# Sidebar layout: put search at the bottom
html_sidebars = {
    '**': [
        'about.html',          # shows logo, project name, description
        'navigation.html',
        'relations.html',
        'searchbox.html',
    ]
}

# Alabaster theme options: center name, show GitHub star button
html_theme_options = {
    'logo_name': True,
    'logo_text_align': 'center',
    'github_user': 'grantjenks',
    'github_repo': 'django-durable',
    'github_button': True,       # show GitHub star button
    'github_type': 'star',
    'github_count': True,
}

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
