# coding: utf-8

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Production"

import sys
from models.mongo_models import *
from nlplib.lemmatize_text import LemmatizeText
from nlplib.named_entities import NamedEntitiesRegonizer
from nlplib.clean_text import CleanText
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor
from langdetect import detect
from indexing.queries import Queries
import names

reload(sys)
sys.setdefaultencoding('utf8')

ct = CleanText()
global mode_global
global language_global

# params:
# elems is list of lists:
#   !!! KEEP THIS FORMAT FOR THE BULK INSERT TO WORK,
#   !!! OTHERWISE YOU WILL OPEN THE GATES OF HELL
#   !!! AND BRING UPON THEE MY WRATH
#   each elem in elems has the following structure:
#   * elem[0] = tweet ID - is the PK
#   * elem[1] = raw text of the document
#   * elem[2] = the date of the tweet
#   * elem[3] = author ID of the tweet
#   * elem[4] = location - coordinates
#   * elem[5] = author age
#   * elem[6] = author gender
#   * elem[7] = language of the tweet, default English

# language, default English
# param mode is for the lemmatizer 0 - fast but not accurate, 1 slow but more accurate
global global_mode

def populateDatabase(elems,dbname='OLAPDB', mode=0):
    global global_mode
    global_mode = mode
    if elems:
        documents = []
        no_threads = cpu_count()
        with ProcessPoolExecutor(max_workers=no_threads) as worker:
            for result in worker.map(processElement, elems):
                if result:
                    documents.append(result)
    if documents:
        queries = Queries(dbname=dbname)
        queries.bulkInsert(documents=documents)

b_idx = 0
g_idx = 0
genderid = {'male': 1, 'female': 2, 'homme': 1, 'femme': 2}
gender = {'homme': 'male', 'femme': 'female'}
authors = dict()
def processElement(elem):
    document = dict()
    # get language
    try:
        lang = detect(elem[1]).upper()
    except Exception as e:
        print e
        lang = ''
    if lang == 'EN':
        # get clean text
        cleanText, hashtags, attags = ct.cleanText(elem[1], lang)
        # if clean text exists
        if len(ct.removePunctuation(cleanText)) > 0:
            # extract lemmas and part of speech
            lemmas = LemmatizeText(rawText=ct.removePunctuation(cleanText), language=lang, mode=global_mode)
            lemmas.createLemmaText()
            lemmaText = lemmas.cleanText
            if lemmaText and lemmaText != " ":
                lemmas.createLemmas()
                words = []
                for w in lemmas.wordList:
                    word = dict()
                    word['word'] = w.word
                    word['tf'] = w.tf
                    word['count'] = w.count
                    word['pos'] = w.wtype
                    words.append(word)

                # named entities:
                ner = NamedEntitiesRegonizer(text=cleanText, language=lang)
                ner.createNamedEntities()
                if ner.ner:
                    document['namedEntities'] = ner.ner

                # construct the document
                document['_id'] = int(elem[0])
                document['rawText'] = elem[1].decode('utf8').encode('utf8').encode('string_escape').replace('\r', '').replace('\n', '')
                document['cleanText'] = cleanText.decode('utf8').encode('utf8').encode('string_escape').replace('\r', '').replace('\n', '')
                document['lemmaText'] = lemmaText
                document['date'] = elem[2]
                document['words'] = words
                # geo location [x, y]
                document['geoLocation'] = elem[4].split(' ')
                # author age
                # this are the change required for the moment when we will keep age as a number
                author = dict()

                # this are the changes required for the moment when we will keep gender as a number
                # author gender - 1 male, 2 female, 0 unknown
                if authors.get(int(elem[3]), -1) == -1:
                    age = elem[5].split('-')
                    author['age'] = (int(age[1]) + int(age[0]))/2
                    author['authorid'] = int(elem[3])
                    author['genderid'] = genderid.get(elem[6], 0)
                    author['gender'] = gender.get(elem[6], 'unknown')
                    if author['genderid'] == 1:
                        global b_idx
                        author['firstname'] = names.boys[b_idx][0]
                        author['lastname'] = names.boys[b_idx][1]
                        b_idx += 1
                    elif author['genderid'] == 2:
                        global g_idx
                        author['firstname'] = names.girls[g_idx][0]
                        author['lastname'] = names.girls[g_idx][1]
                        g_idx += 1
                    authors[int(elem[3])] = author
                else:
                    author = authors[int(elem[3])]

                # print author
                document['authors'] = [author]

                if attags:
                    document['attags'] = attags
                if hashtags:
                    document['hashtags'] = hashtags
    return document