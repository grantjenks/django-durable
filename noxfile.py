"""Nox sessions for testing, linting, formatting, and docs."""

import nox

nox.options.sessions = ('lint', 'tests', 'docs')


@nox.session(venv_backend='uv')
def lint(session: nox.Session) -> None:
    """Run static analysis."""
    session.install('ruff', 'isort')
    session.run('ruff', 'check', '.')
    session.run('isort', '--check-only', '.')


@nox.session(venv_backend='uv')
def format(session: nox.Session) -> None:
    """Format the code."""
    session.install('ruff', 'isort')
    session.run('ruff', 'format', '.')
    session.run('isort', '.')


@nox.session(venv_backend='uv')
def tests(session: nox.Session) -> None:
    """Run the test suite."""
    session.install('.[dev]')
    session.run('python', 'manage.py', 'migrate', '--noinput')
    session.run('pytest')


@nox.session(venv_backend='uv')
def docs(session: nox.Session) -> None:
    """Build the documentation."""
    session.install('sphinx', 'myst-parser')
    session.run('sphinx-build', '-b', 'html', 'docs', 'docs/_build')
