import logging
from os.path import join, dirname
from typing import Tuple

from neo4j import Session

from dbpediasampler.sampling.io import open_index_manager, open_statement_writer


class Sampler:
    """A sampler for subsampling DBpedia knowledge graph. It uses the specified
    session to a Neo4J instance to query the KG for subsampling."""

    def __init__(self, data_dir: str, session: Session):
        """creates a new sampler.

        :param data_dir: path to the directory to which the results shall be
        writen.
        :param session: session to Neo4J.
        """
        self._data_dir = data_dir
        self._session = session
        self._label_query = None
        self._stmt_query = None

    @property
    def label_query(self):
        if self._label_query is None:
            with open(join(dirname(__file__), 'query', 'label.query')) \
                    as query_f:
                self._label_query = query_f.read()
        return self._label_query

    @property
    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query', 'statements.query')) \
                    as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query

    def run(self):
        """runs the sampler."""
        with open_index_manager(join(self._data_dir, 'index.tsv.gz'),
                                join(self._data_dir, 'relevant_entities.tsv.gz')
                                ) as index_manager:
            with open_statement_writer(index_manager, join(self._data_dir,
                                                           'statements.nt.gz'),
                                       join(self._data_dir, 'statements.tsv.gz')
                                       ) as stmt_writer:
                lw = index_manager.label_writer
                n = 0
                for stmt in self.fetch_statements():
                    stmt_writer.add_statement(stmt)
                    for ent in [stmt[0], stmt[2]]:
                        if not lw.is_present(ent):
                            label, desc, thumb = self.fetch_label(ent)
                            lw.write_label(ent, label, desc, thumb)
                    n += 1
                    if n % 100000 == 0:
                        logging.info('Loaded %s statements.' % n)
                logging.info('Successfully loaded %s statements.' % n)

    def fetch_label(self, uri: str) -> Tuple[str, str, str]:
        """fetches label for the given entity.

        :param uri: URI of the entity for which the label shall be fetched.
        """
        result = self._session.run(self.label_query, uri=uri)
        record = result.single()
        return record['label'], record['description'], record['depiction']

    def fetch_statements(self):
        """fetches the statements that should be included in the subsampled KG.

        :return: an iterator over the statements.
        """
        result = self._session.run(self.statement_query)
        for record in result:
            yield record['subj'], record['pred'], record['obj']
