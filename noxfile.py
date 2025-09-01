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
    # Install package (with dev extras) so autodoc + Django can import settings
    session.install('.[dev]')
    session.run('sphinx-build', '-b', 'html', 'docs', 'docs/_build')


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

    # Ensure docs exist; if not, guide the user.
    index_html = 'docs/_build/html/index.html'
    if not session.run('test', '-f', index_html, external=True, success_codes=(0, 1)) == 0:
        session.error('Docs not found. Run: nox -s docs')

    session.run(
        'rsync',
        '--rsync-path',
        rsync_path,
        '-azP',
        '--stats',
        '--delete',
        'docs/_build/html/',
        dest,
        external=True,
    )
