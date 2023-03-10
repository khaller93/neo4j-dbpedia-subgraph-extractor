import logging
from abc import ABC, abstractmethod
from os.path import join, dirname
from typing import Tuple

from neo4j import Session

from extractor.io import open_index_manager, open_statement_writer

LOAD_LIMIT = 1000000


class Extractor(ABC):
    """An extractor for subgraphs from DBpedia knowledge graphs. It uses the
    specified session to a Neo4J instance to query the KG for subsampling."""

    def __init__(self, data_dir: str, session: Session):
        """creates a new sampler.

        :param data_dir: path to the directory to which the results shall be
        writen.
        :param session: session to Neo4J.
        """
        self._data_dir = data_dir
        self._session = session
        self._label_query = None

    def label_query(self):
        """gets the query for fetching labels for sampled entities."""
        if self._label_query is None:
            with open(join(dirname(__file__), 'query', 'label.query')) \
                    as query_f:
                self._label_query = query_f.read()
        return self._label_query

    @abstractmethod
    def statement_query(self) -> str:
        """gets the query for fetching sampled statements."""
        raise NotImplementedError('must be implemented')

    def run(self):
        """runs the extractor."""
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
                    if n % LOAD_LIMIT == 0:
                        logging.info('Loaded %s statements.' % n)
                logging.info('Successfully loaded %s statements.' % n)

    def fetch_label(self, uri: str) -> Tuple[str, str, str]:
        """fetches label for the given entity.

        :param uri: URI of the entity for which the label shall be fetched.
        """
        result = self._session.run(self.label_query(), uri=uri)
        record = result.single()
        return record['label'], record['description'], record['depiction']

    def fetch_statements(self):
        """fetches the statements that should be included in the subsampled KG.

        :return: an iterator over the statements.
        """
        skip = 0
        while True:
            result = self._session.run(self.statement_query(),
                                       skip=skip, limit=LOAD_LIMIT)
            if result.peek() is None:
                break
            for record in result:
                yield record['subj'], record['pred'], record['obj']
            skip += LOAD_LIMIT


class DB35MExtractor(Extractor):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)
        self._stmt_query = None

    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query',
                           'dbpedia35m_statements.query')) as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query


class DB1MExtractor(Extractor):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)
        self._stmt_query = None

    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query',
                           'dbpedia1m_statements.query')) as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query


class DB500KExtractor(Extractor):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)
        self._stmt_query = None

    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query',
                           'dbpedia500k_statements.query')) as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query


class DB250KExtractor(Extractor):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)
        self._stmt_query = None

    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query',
                           'dbpedia250k_statements.query')) as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query


class DBA240Extractor(Extractor):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)
        self._stmt_query = None

    def statement_query(self):
        if self._stmt_query is None:
            with open(join(dirname(__file__), 'query',
                           'dbpediaA240_statements.query')) as query_f:
                self._stmt_query = query_f.read()
        return self._stmt_query