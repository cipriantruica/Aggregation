# coding: utf-8

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Production"

import pymongo
from ne_index import NEIndex
from vocabulary_index import VocabularyIndex

class Queries:
    def __init__(self, dbname):
        client = pymongo.MongoClient()
        self.dbname = dbname
        self.db = client[self.dbname]

    def countDocuments(self, query=None):
        if query:
            return self.db.documents.find(query).count()
        else:
            return self.db.documents.find().count()

    def getWords(self, query=False, limit=150):
        if query:
            return self.db.vocabulary_query.find(fields={'word':1,'idf':1},limit=limit, sort=[('idf',pymongo.ASCENDING)])
        else:
            return self.db.vocabulary.find(fields={'word':1,'idf':1},limit=limit, sort=[('idf',pymongo.ASCENDING)])

    def getNamedEntities(self, query=None, limit=None):
        if query:
            # if there is a query then we construct a smaller NE_INDEX
            query_ner = {'namedEntities': {'$exists': 'true'}}
            query_ner.update(query)
            ne = NEIndex(dbname)
            ne.createIndex(query_ner)
            if limit:
                return self.db.named_entities_query.find(sort=[('count',pymongo.DESCENDING)], limit=limit)
            else:
                return self.db.named_entities_query.find(sort=[('count',pymongo.DESCENDING)])
        else:
            # use the already build NE_INDEX
            if limit:
                return self.db.named_entities.find(sort=[('count',pymongo.DESCENDING)], limit=limit)
            else:
                return self.db.named_entities.find(sort=[('count',pymongo.DESCENDING)])

    def constructVocabulary(self, query=None):
        vocab = VocabularyIndex(self.dbname)
        if query:
            vocab.createIndex(query)
        else:
            vocab.createIndex()

    def bulkInsert(self, documents):
        try:
            self.db.documents.insert(documents, continue_on_error=True)
        except pymongo.errors.DuplicateKeyError:
            pass