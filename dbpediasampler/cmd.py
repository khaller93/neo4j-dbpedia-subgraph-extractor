from os import getenv, makedirs, getcwd
from os.path import join, exists

from typer import Typer
from neo4j import GraphDatabase

from dbpediasampler.sampling.dbpedia1m.dbpedia1m import DBpedia1mSampler

app = Typer()


def _default_data_dir_path(dataset_name: str):
    """default path to the the data directory. where results will be stored.

    :param dataset_name: name of the dataset for which to create the default
    data directory path.
    """
    return join(getcwd(), 'data', dataset_name)


@app.command(name='dbpedia1m', help='using dbpedia1m methodology to sample '
                                    'DBpedia KG')
def run_dbpedia1m(data_dir: str = _default_data_dir_path('dbpedia1m'),
                  host: str = getenv('NEO4J_HOSTNAME', default='localhost'),
                  port: int = getenv('NEO4J_BOLT_PORT', default=7687),
                  username: str = getenv('NEO4J_USERNAME', default='neo4j'),
                  password: str = getenv('NEO4J_PASSWORD', default='neo4j')):
    """

    :param data_dir: path to the the data directory. where results will be
    stored.
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
            sampler = DBpedia1mSampler(data_dir, session)
            sampler.run()
    finally:
        driver.close()
