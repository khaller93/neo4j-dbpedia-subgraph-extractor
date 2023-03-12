import logging
from os import getenv, getcwd, makedirs
from os.path import join, exists

from neo4j import GraphDatabase
from typer import Typer

from extractor import DB1MExtractor, DB35MExtractor, DB250KExtractor

app = Typer()

extractors = {
    'db35m': DB35MExtractor,
    'db1m': DB1MExtractor,
    'db250k': DB250KExtractor,
}


def _default_data_dir_path() -> str:
    """default path to the data directory. where results will be stored.

    :return: default path to the data directory. where results will be stored.
    """
    return join(getcwd(), 'data')


def run_sampling(clazz, data_dir: str, host: str, port: int, username: str,
                 password: str) -> None:
    """runs the specified DBpedia sampling methodology.

    :param clazz: DBpedia sampling class.
    :param data_dir: path to the data directory. where results will be stored.
    :param host: of the Neo4J instance maintaining the DBpedia KG.
    :param port: bolt port of the Neo4J instance maintaining the DBpedia KG.
    :param username: username of the Neo4J instance maintaining the DBpedia KG.
    :param password: password of the Neo4J instance maintaining the DBpedia KG.
    """
    driver = GraphDatabase.driver('neo4j://%s:%d' % (host, port),
                                  auth=(username, password))
    try:
        with driver.session(database='neo4j') as session:
            if not exists(data_dir):
                makedirs(data_dir)
            sampler = clazz(data_dir, session)
            sampler.run()
    finally:
        driver.close()


@app.command(name='extract', help='extract subgraph with specified label/name')
def extract_dataset(dataset_name: str,
                    data_dir: str = _default_data_dir_path(),
                    host: str = getenv('NEO4J_HOSTNAME', default='localhost'),
                    port: int = getenv('NEO4J_BOLT_PORT', default=7687),
                    username: str = getenv('NEO4J_USERNAME', default='neo4j'),
                    password: str = getenv('NEO4J_PASSWORD', default='neo4j')):
    dataset_name = dataset_name.lower().strip()
    if dataset_name in extractors:
        run_sampling(extractors[dataset_name], join(data_dir, dataset_name),
                     host, port, username, password)
    else:
        raise ValueError('the dataset with name "%s" is unknown' % dataset_name)


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
