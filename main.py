import logging
from os import getenv

from typer import Typer

from dbpediasampler import cmd

app = Typer()
app.add_typer(cmd.app, name='sample')


def _configure_logging():
    """configures the logger for this application. The log level can be changed
     by setting the environment variable `LOG_LEVEL` in the execution
     environment of this application."""
    log_level = getenv('LOG_LEVEL', default='INFO')
    logging.basicConfig(format='%(levelname)-8s :: %(message)s',
                        level=log_level)


if __name__ == '__main__':
    _configure_logging()
    app()
