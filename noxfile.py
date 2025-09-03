"""Nox sessions for testing, linting, formatting, and docs."""

from pathlib import Path

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
    # Install package (with dev extras) so autodoc + Django can import settings
    session.install('.[dev]')
    session.run('sphinx-build', '-b', 'html', 'docs', 'docs/_build/html')


@nox.session(venv_backend='uv')
def bench(session: nox.Session) -> None:
    """Run the benchmark."""
    session.install('.')
    session.run('python', 'testproj/benchmark.py', *session.posargs)


@nox.session(venv_backend='uv')
def upload(session: nox.Session) -> None:
    """Upload built docs to public/docs/django-durable/ via rsync.

    Usage:
      nox -s upload

    Notes:
    - Expects docs already built at docs/_build/html.
    - Uses remote host 'grantjenks' and rsync via 'sudo -u herokuish rsync'.
    - Destination: /srv/www/grantjenks.com/public/docs/django-durable/
    """
    dest = 'grantjenks:/srv/www/grantjenks.com/public/docs/django-durable/'
    rsync_path = 'sudo -u herokuish rsync'

    # Allow overriding destination by passing a single positional arg.
    if session.posargs:
        dest = session.posargs[0]

    # Determine built docs directory (prefer html subdir).
    out_html = Path('docs/_build/html')
    out_root = Path('docs/_build')
    if (out_html / 'index.html').is_file():
        src = str(out_html) + '/'
    elif (out_root / 'index.html').is_file():
        src = str(out_root) + '/'
    else:
        session.error('Docs not found. Run: nox -s docs')

    session.run(
        'rsync',
        '--rsync-path',
        rsync_path,
        '-azP',
        '--stats',
        '--delete',
        src,
        dest,
        external=True,
    )
