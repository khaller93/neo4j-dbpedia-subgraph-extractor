from abc import ABC, abstractmethod
from os.path import join

from neo4j import Session

from dbpediasampler.sampling.io import open_index_manager, open_statement_writer


class Sampler(ABC):
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

    def run(self):
        """runs the sampler."""
        with open_index_manager(join(self._data_dir, 'index.tsv.gz'),
                                join(self._data_dir, 'relevant_entities.tsv.gz')
                                ) as index_manager:
            with open_statement_writer(index_manager, join(self._data_dir,
                                                           'statements.nt.gz'),
                                       join(self._data_dir, 'statements.tsv.gz')
                                       ) as stmt_writer:
                for stmt in self.fetch_statements():
                    stmt_writer.add_statement(stmt)

    @abstractmethod
    def fetch_statements(self):
        """fetches the statements that should be included in the subsampled KG.

        :return: an iterator over the statements.
        """
        raise NotImplementedError('must be implemented by sampling strategy')
