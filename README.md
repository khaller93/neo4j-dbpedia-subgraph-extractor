# DBpedia Subgraph Extractor from Neo4J

The application in this repository can be used to fetch the statements of a
labelled subgraph in Neo4J. It writes them into a format that is understandable
by our graph data science pipeline.

This application is specifically designed for our
[Neo4J instance of DBpedia KG from 2022-09](https://github.com/khaller93/dbpedia-kg-neo4j/tree/sampling),
which contains a number of labeled subgraphs, namely, `DB35M`, `DB1M`, `DB250k`,
and `DBA240`. By specifying the label, the application will generate the
following files for the subgraph with this specified label:

* `index.tsv.gz` is a file in tabular format that maps a simple integer to a
  URI, which identifies an entity in the KG.
  
* `index_labels.tsv.gz` is a file that links entities (represented by their
  index number) to their label and description.
  
* `relevant_entities.tsv.gz` is a file with all the entities, which occur as
  subject or/and as object in statements of the subsampled KG.
  
* `statements.tsv.gz` is a file with all the statements of the subsampled KG.
  The first column contains the subjects, second column the predicates, and the
  third column the objects. All those entities are represented by their index
  number (see `index.tsv.gz`) and not their URI.

## Installation

Requires a recent version of Python, and the packages specified in
`requirements.txt`. You can install them with `pip` like this:

```bash
pip install -r requirements.txt
```

## Run

```
Usage: main.py [OPTIONS] DATASET_NAME

  extract subgraph with specified label/name

Arguments:
  DATASET_NAME  [required]

Options:
  --data-dir TEXT       [default: ./data]
  --host TEXT           [default: localhost]
  --port INTEGER        [default: 7687]
  --username TEXT       [default: neo4j]
  --password TEXT       [default: neo4j]
```

Example: `python main.py db1m --username neo4j --password test`

## Contact

* Kevin Haller ([contact@kevinhaller.dev](mailto:contact@kevinhaller.dev))