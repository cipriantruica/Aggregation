# coding: utf-8

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Production"

from gensim.utils import lemmatize
from nlplib.clean_text import CleanText
from indexing.queries import Queries
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import math
import time

cleanText = CleanText()

class Search:
    def __init__(self, searchPhrase, dbname='TwitterDB', query=None, k=0):
        self.queries = Queries(dbname)
        self.words = [word.split('/')[0] for word in lemmatize(cleanText.removeStopWords(cleanText.cleanText(searchPhrase)[0]))]
        self.idfs = dict()
        and_list = []
        for word in self.words:
            and_list.append({'words.word': word})
        self.query_search = {"$and" : and_list}
        if query:
            self.existing = True
            self.query_search.update(query)
        else:
            self.existing = False
        self.k = k


    def results(self):
        # get ids for the search words
        idf = self.queries.getWords(query={'word': {"$in": self.words}}, fields={'word': 1, 'IDF': 1}, existing=self.existing)
        for word in idf:
            self.idfs[word['word']] = word['IDF']
        # get documents
        fields = {'_id': 1, 'author': 1, 'date': 1, 'rawText': 1, 'words.word': 1, 'words.tf': 1}
        documents = self.queries.getDocuments(query=self.query_search, fields=fields)
        list_documents = []
        # parallelized version -  doesn't work yet
        # no_threads = cpu_count()
        # with ProcessPoolExecutor(max_workers=no_threads) as worker:
        #         for result in worker.map(self.process, documents):
        #             if result:
        #                 list_documents.append(result)

        # serial version
        for doc in documents:
            document = dict()
            document['id'] = doc['_id']
            document['rawText'] = doc['rawText']
            document['author'] = doc['author']
            document['date'] = doc['date']
            score = 0
            for word in self.idfs:
                tf = 1
                for tfs in doc['words']:
                    if tfs.get(word, 1) == 1:
                        tf = tfs['tf']
                score += tf*self.idfs[word];
            document['score'] = math.log(1+score, 2)
            list_documents.append(document)
            list_documents.append(self.process(doc))

        return list_documents

    def process(self, elem):
        document = dict()
        document['id'] = elem['_id']
        document['rawText'] = elem['rawText']
        document['author'] = elem['author']
        document['date'] = elem['date']
        score = 0
        for word in self.idfs:
            tf = 1
            for tfs in elem['words']:
                if tfs.get(word, 1) == 1:
                    tf = tfs['tf']
            score += tf*self.idfs[word]
        document['score'] = math.log(1+score, 2)
        return document


if __name__ == "__main__":
    start = time.time()
    searchPhrase = "like get"
    search = Search(searchPhrase=searchPhrase, dbname='TwitterDB', query=None)
    for elem in search.results():
        print elem
    end = time.time()
    print 'Time', (end - start)