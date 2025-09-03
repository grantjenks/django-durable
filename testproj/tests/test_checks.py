import os
import sys
from pathlib import Path

import django

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testproj.settings")
django.setup()

from django.core.checks import run_checks
from django_durable.registry import register


def test_warns_on_nondeterministic_code():
    @register.workflow()
    def random_wf(ctx):
        import random
        return random.random()

    errors = run_checks()

    assert any(
        e.id == "django_durable.W001" and "random" in e.msg for e in errors
    )

    register.workflows.pop("testproj.random_wf", None)
