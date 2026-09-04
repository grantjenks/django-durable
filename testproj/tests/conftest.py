"""Initialize Django independently of test collection order."""
import os

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'testproj.settings')
django.setup()
