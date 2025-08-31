"""Nox sessions for testing and linting."""

import nox

nox.options.sessions = ('lint', 'tests')


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
    session.install('pytest')
    session.run('pytest')
