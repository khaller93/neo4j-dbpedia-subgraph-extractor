import csv
import gzip
from typing import Tuple

from rdflib.term import URIRef


class _IndexManager:
    """"""

    def __init__(self, index_stream, rev_entities_stream):
        self._last_index = 0
        self._entity_map = {}
        self._index_w = csv.writer(index_stream, delimiter='\t')
        self._rev_entities_w = csv.writer(rev_entities_stream, delimiter='\t')

    def add_entry_and_get_index(self, uri: str, relevant: bool) -> int:
        """adds the given URI to the index, if it hasn't already an index.

        :param uri: URI of the entity which shall be added.
        :param relevant: `True`, if the URI is a relevant object,
        otherwise `False`.
        :return: the index number of the URI.
        """
        if uri not in self._entity_map:
            if relevant:
                self._rev_entities_w.writerow([self._last_index])
            self._index_w.writerow([self._last_index, uri])
            self._entity_map[uri] = self._last_index
            self._last_index += 1
        return self._entity_map[uri]


class IndexManagerIO:
    """"""

    def __init__(self, index_f_path: str, rev_entities_path: str):
        """creates a new index writer for entities.

        :param index_f_path: path to the index file.
        :param rev_entities_path: path to the relevant entities file.
        """
        self._ent_index_w = gzip.open(filename=index_f_path, mode='wt')
        self._rev_ent_w = gzip.open(filename=rev_entities_path, mode='wt')
        self._im = _IndexManager(self._ent_index_w, self._rev_ent_w)

    def __enter__(self):
        return self._im

    def add_entry_and_get_index(self, uri: str, relevant: bool) -> int:
        """adds the given URI to the index, if it hasn't already an index.

        :param uri: URI of the entity which shall be added.
        :param relevant: `True`, if the URI is a relevant object,
        otherwise `False`.
        :return: the index number of the URI.
        """
        return self._im.add_entry_and_get_index(uri, relevant)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._ent_index_w is not None:
            self._ent_index_w.flush()
            self._ent_index_w.close()
        if self._rev_ent_w is not None:
            self._rev_ent_w.flush()
            self._rev_ent_w.close()


def open_index_manager(index_f_path: str,
                       rev_entities_path: str) -> IndexManagerIO:
    """creates a new index manager for entities.

    :param index_f_path: path to the index file.
    :param rev_entities_path: path to the relevant entities file.
    :return: index manager for entities
    """
    return IndexManagerIO(index_f_path, rev_entities_path)


class _StatementWriter:

    def __init__(self, index_mng: IndexManagerIO, statement_rdf_stream,
                 statement_tsv_stream):
        self._im = index_mng
        self._statement_rdf_stream = statement_rdf_stream
        self._statement_tsv_w = csv.writer(statement_tsv_stream, delimiter='\t')

    def add_statement(self, stmt: Tuple[str, str, str]):
        """adds the given statement to be writen to disk.

        :param stmt: that shall be writen.
        """
        s = self._im.add_entry_and_get_index(stmt[0], relevant=True)
        p = self._im.add_entry_and_get_index(stmt[1], relevant=False)
        o = self._im.add_entry_and_get_index(stmt[2], relevant=True)
        self._statement_tsv_w.writerow([s, p, o])
        self._statement_rdf_stream.write('%s %s %s .\n' % (URIRef(stmt[0]).n3(),
                                                           URIRef(stmt[1]).n3(),
                                                           URIRef(stmt[2]).n3())
                                         )


class StatementWriterIO:

    def __init__(self, index_mng: IndexManagerIO, statement_rdf_f_path: str,
                 statement_tsv_f_path: str):
        self._statement_rdf_w = gzip.open(filename=statement_rdf_f_path,
                                          mode='wt')
        self._statement_tsv_w = gzip.open(filename=statement_tsv_f_path,
                                          mode='wt')
        self._sw = _StatementWriter(index_mng, self._statement_rdf_w,
                                    self._statement_tsv_w)

    def __enter__(self):
        return self._sw

    def add_statement(self, stmt: Tuple[str, str, str]):
        """adds the given statement to be writen to disk.

        :param stmt: that shall be writen.
        """
        return self._sw.add_statement(stmt)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._statement_rdf_w is not None:
            self._statement_rdf_w.close()
        if self._statement_tsv_w is not None:
            self._statement_tsv_w.close()


def open_statement_writer(index_mng: IndexManagerIO,
                          statement_rdf_f_path: str,
                          statement_tsv_f_path: str) -> StatementWriterIO:
    return StatementWriterIO(index_mng, statement_rdf_f_path,
                             statement_tsv_f_path)
