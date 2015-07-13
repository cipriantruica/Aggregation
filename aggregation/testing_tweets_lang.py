# coding: utf-8

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Production"

import time
import utils
from ddl_mongo_lang import *
from models.mongo_models import *
from indexing.queries import Queries


def populateDB(filename, csv_delimiter, header, language='EN', dbname='TwitterDB', mode=0):
    start = time.time() 
    h, lines = utils.readCSV(filename, csv_delimiter, header)
    populateDatabase(lines, dbname)
    end = time.time() 
    print "time_populate.append(", (end - start), ")"

def constructIndexes(dbname):
    #build Vocabulary
    queries = Queries(dbname=dbname)
    start = time.time()
    queries.constructVocabulary()
    end = time.time()
    print "vocabulary_build.append(", (end - start) , ")"

    # built the NE Index
    start = time.time()
    queries.constructNamedEntities()
    end = time.time()
    print "ne_build.append(", (end - start) , ")"


def main(filename, csv_delimiter = '\t', header = True, dbname = 'TwitterDB', language='EN', initialize = 0, mode=0):
    connectDB(dbname)
    # initialize everything from the stat
    if initialize == 0:
        Documents.drop_collection()
    populateDB(filename, csv_delimiter, header, language, dbname=dbname, mode=mode)
    constructIndexes(dbname)

# this script receives 7 parameters
# 1 - filename
# 2 - the csv delimiter: t - tab, c - coma, s - semicolon
# 3 - integer: 1 csv has header, 0 csv does not have hearer
# 4 - integer: - nr of threads
# 5 - language: EN or FR
# 6 - integer: 0 - create the database, 1 - update the database
# 7 - integer: 0 - use fast lemmatizer (not accurate), 1 - use slow lemmatizer (accurate)
if __name__ == "__main__":
    filename = sys.argv[1] 
    csv_delimiter = utils.determineDelimiter(sys.argv[2])
    header = bool(sys.argv[3])
    dbname = sys.argv[4]
    language = sys.argv[5] #currently EN & FR, FR does not work so well
    initialize = int(sys.argv[6])
    mode = int(sys.argv[7])
    main(filename=filename, csv_delimiter=csv_delimiter, header=header, dbname=dbname, language=language, initialize=initialize, mode=mode)
