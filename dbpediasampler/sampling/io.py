import csv
import gzip

from typing import Tuple, Union

from rdflib.term import URIRef


class LabelWriter:
    """a writer for labels and descriptions of an entity"""

    def __init__(self, index_mng: 'IndexManager', tsv_writer):
        """creates a new label writer.

        :param index_mng: manager to use for getting the index number for URIs.
        :param tsv_writer: TSV writer for writing rows of entities with their
        label and description.
        """
        self._index_mng = index_mng
        self._tsv_writer = tsv_writer
        self._labelled_set = set()

    def is_present(self, uri: str) -> bool:
        """checks whether a label has already been written for a given entity.

        :param uri: URI of the entity for which it should be checked.
        :return: `True`, if labels have already been written for a given entity,
        otherwise `False`.
        """
        i = self._index_mng.get_index(uri)
        if i is None:
            raise ValueError('entity "%s" wasn\'t indexed')
        return i in self._labelled_set

    def write_label(self, uri: str, label: str, description: str) -> None:
        """writes the label and description to a file for a given entity.

        :param uri: uri of the entity for which the label shall be written.
        :param label: label that shall be written for the given entity.
        :param description: description that shall be written for the given
        entity.
        """
        i = self._index_mng.get_index(uri)
        if i is None:
            raise ValueError('entity "%s" wasn\'t indexed')
        if i not in self._labelled_set:
            self._tsv_writer.writerow([i, label, description])
            self._labelled_set.add(i)


class IndexManager:
    """manager that maintains the index for entities, and writes it in form
    of tab separated value lines to disk."""

    def __init__(self, index_stream, index_label_stream, rev_entities_stream):
        """creates a new index manager for entities.

        :param index_stream: stream to which the index shall be written.
        :param index_label_stream: stream to which the labels shall be written.
        :param rev_entities_stream: stream to which relevant entities shall be
        written.
        """
        self._last_index = 0
        self._entity_map = {}
        self._index_w = csv.writer(index_stream, delimiter='\t')
        self._rev_entities_w = csv.writer(rev_entities_stream, delimiter='\t')
        self.label_writer = LabelWriter(self, csv.writer(index_label_stream,
                                                         delimiter='\t'))

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

    def get_index(self, uri: str) -> Union[None, int]:
        """aims to get the index for the uri of an entity. The index will be
        returned, or `None`, if there is no index for this entity.

        :return: index for an entity, `None`, if there is no index for this
        entity.
        """
        return self._entity_map[uri] if uri in self._entity_map else None


class IndexManagerFileIO(IndexManager):
    """an index manager that writes the data to GZIP compressed files."""

    def __init__(self, index_f_path: str, index_label_f_path: str,
                 rev_entities_f_path: str):
        """creates a new index manager for entities.

        :param index_f_path: path to the index file.
        :param index_label_f_path: path to the file with the labels.
        :param rev_entities_f_path: path to the relevant entities file.
        """
        self._ent_index_w = gzip.open(filename=index_f_path, mode='wt')
        self._index_label_w = gzip.open(filename=index_label_f_path, mode='wt')
        self._rev_ent_w = gzip.open(filename=rev_entities_f_path, mode='wt')
        super().__init__(self._ent_index_w, self._index_label_w,
                         self._rev_ent_w)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._ent_index_w is not None:
            self._ent_index_w.flush()
            self._ent_index_w.close()
        if self._rev_ent_w is not None:
            self._rev_ent_w.flush()
            self._rev_ent_w.close()


def open_index_manager(index_f_path: str,
                       rev_entities_path: str) -> IndexManager:
    """creates a new index manager for entities.

    :param index_f_path: path to the index file.
    :param rev_entities_path: path to the relevant entities file.
    :return: index manager for entities.
    """
    # create index labels path from index path
    f_s = index_f_path.split('.')
    f_s[0] = f_s[0] + '_labels'
    # construct index manager
    return IndexManagerFileIO(index_f_path, '.'.join(f_s), rev_entities_path)


class StatementWriter:
    """a writer for sampled statements of a KG."""

    def __init__(self, index_mng: IndexManager, statement_rdf_stream,
                 statement_tsv_stream):
        """creates a new statements writer with which triples are written to
        disk in multiple formats.

        :param index_mng: manager to use for getting the index number for URIs.
        :param statement_rdf_stream: stream to which triples are written in
        N-Triples format (RDF).
        :param statement_tsv_stream: stream to which triples are written in
        tab-separated value lines.
        """
        self._im = index_mng
        self._statement_rdf_stream = statement_rdf_stream
        self._statement_tsv_w = csv.writer(statement_tsv_stream, delimiter='\t')

    def add_statement(self, stmt: Tuple[str, str, str]) -> None:
        """adds the given statement to be writen to disk.

        :param stmt: statement in form of a triple that shall be writen.
        """
        s = self._im.add_entry_and_get_index(stmt[0], relevant=True)
        p = self._im.add_entry_and_get_index(stmt[1], relevant=False)
        o = self._im.add_entry_and_get_index(stmt[2], relevant=True)
        self._statement_tsv_w.writerow([s, p, o])
        n_triple = '%s %s %s .\n' % (URIRef(stmt[0]).n3(), URIRef(stmt[1]).n3(),
                                     URIRef(stmt[2]).n3())
        self._statement_rdf_stream.write(n_triple)


class StatementWriterFileIO(StatementWriter):
    """a writer for sampled statements of a KG that writes the data to GZIP
    compressed files."""

    def __init__(self, index_mng: IndexManager,
                 statement_rdf_f_path: str, statement_tsv_f_path: str):
        """creates a statement writer with which triples are written to
        disk in multiple formats.

        :param index_mng: manager to use for getting the index number for URIs.
        :param statement_rdf_f_path: path to the RDF file for sampled
        statements.
        :param statement_tsv_f_path: path to the TSV file for sampled
        statements.
        """
        self._stmt_rdf_w = gzip.open(filename=statement_rdf_f_path, mode='wt')
        self._stmt_tsv_w = gzip.open(filename=statement_tsv_f_path, mode='wt')
        super().__init__(index_mng, self._stmt_rdf_w, self._stmt_tsv_w)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._stmt_rdf_w is not None:
            self._stmt_rdf_w.close()
        if self._stmt_tsv_w is not None:
            self._stmt_tsv_w.close()


def open_statement_writer(index_mng: IndexManager, statement_rdf_f_path: str,
                          statement_tsv_f_path: str) -> StatementWriterFileIO:
    """creates a new writer for sampled statements of a KG.

    :param index_mng:  manager to use for getting the index number for URIs.
    :param statement_rdf_f_path: path to the RDF file for sampled statements.
    :param statement_tsv_f_path: path to the TSV file for sampled statements.
    :return: a statement writer for sampled statements of a KG.
    """
    return StatementWriterFileIO(index_mng, statement_rdf_f_path,
                                 statement_tsv_f_path)
