from os.path import join, dirname

from neo4j import Session

from dbpediasampler.sampling.sampler import Sampler


class DBpedia1mSampler(Sampler):

    def __init__(self, data_dir: str, session: Session):
        super().__init__(data_dir, session)

    def fetch_statements(self):
        with open(join(dirname(__file__), 'query',
                       'statements.query')) as query_f:
            query = query_f.read()
            result = self._session.run(query)
            for record in result:
                yield record['subj'], record['pred'], record['obj']
