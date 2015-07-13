# coding: utf-8

import pymongo
import codecs

client = pymongo.MongoClient()
dbname = 'OLAPDB'
db = client[dbname]


pos_id = {
    'PR': 0,
    'MD': 1,
    'VB': 2,
    'WD': 3,
    'RP': 4,
    'PD': 5,
    'NN': 6,
    'FW': 7,
    'CC': 8,
    'LS': 9,
    '-N': 10,
    'CD': 11,
    'TO': 12,
    'UH': 13,
    'RB': 14,
    'IN': 15,
    'WP': 16,
    'JJ': 17,
    'DT': 18,
    'WR': 19
}
cursor = db.documents.find()
authors = dict()
attags = dict()
hashtags = dict()
words = dict()

doc_f = codecs.open('01_documents.sql', 'w', 'utf-8')
auth_f = codecs.open('02_authors.sql', 'w', 'utf-8')
lf1 = codecs.open('03_documents_authors.sql', 'w', 'utf-8')
hash_f = codecs.open('04_hashtags.sql', 'w', 'utf-8')
lf2 = codecs.open('05_documents_hashtags.sql', 'w', 'utf-8')
at_f = codecs.open('06_attags.sql', 'w', 'utf-8')
lf3 = codecs.open('07_documents_attags.sql', 'w', 'utf-8')
words_f = codecs.open('08_words.sql', 'w', 'utf-8')
vocab_f = codecs.open('09_vocabulary.sql', 'w', 'utf-8')
lf4 = codecs.open('10_vocabulary_pos.sql', 'w', 'utf-8')

idx = 0
for elem in cursor:
    author = dict()
    rawText = elem['rawText'].encode('utf8').encode('string_escape').replace('\r', '').replace('\n', '').replace("\\", "").replace("\"", "")
    rawText = rawText.replace('\'', '\'\'')
    cleanText = elem['cleanText'].encode('utf8').encode('string_escape').replace('\r', '').replace('\n', '').replace("\\", "").replace("\"", "")
    cleanText = cleanText.replace('\'', '\'\'')
    # doc_f.write('insert into documents(id, rawtext, cleantext, lemmatext, documentdate, languageid) values('+ str(elem['_id']) + ' , \'' + rawText + '\' , \'' + cleanText + '\' , \'' + elem['lemmaText'] + '\' , \'' + elem['date'] +'\' , 1);\n')
    for a in elem['authors']:
        if authors.get(a['authorid'], -1) == -1:
            author['documentid'] = [elem['_id']]
            author['age'] = a['age']
            author['firstname'] = a['firstname']
            author['lastname'] = a['lastname']
            author['genderid'] = a['genderid']
            authors[a['authorid']] = author
        else:
            authors[a['authorid']]['documentid'] += [elem['_id']]
    if elem.get('hashtags', -1) != -1:
        for ht in elem['hashtags']:
            if hashtags.get(ht, -1) == -1:
                hashtags[ht] = [elem['_id']]
            else:
                hashtags[ht] += [elem['_id']]
    if elem.get('attags', -1) != -1:
        for at in elem['attags']:
            if attags.get(at, -1) == -1:
                attags[at] = [elem['_id']]
            else:
                attags[at] += [elem['_id']]
    for w in elem['words']:
        word = dict()
        word['documentid'] = elem['_id']
        word['count'] = w['count']
        word['tf'] = w['tf']
        word['pos'] = w['pos']
        if words.get(w['word'], -1) == -1:
            words[w['word']] = [word]
        else:
            words[w['word']] += [word]

for key in authors:
    auth_f.write('insert into authors(id, age, firstname, lastname, genderid) values('+ str(key) + ' , ' + str(authors[key]['age']) + ' , \'' + authors[key]['firstname'] + '\' , \'' + authors[key]['lastname'] + '\' , ' + str(authors[key]['genderid']) +');\n')
    for docid in authors[key]['documentid']:
        lf1.write('insert into documents_authors(documentid, authorid) values('+ str(docid) + ' , ' + str(key) + ');\n')

idx_ht = 0
for key in hashtags:
    hash_f.write('insert into hashtags(id, hashtag) values('+ str(idx_ht) + ' , \'' + key + '\');\n')
    for docid in hashtags[key]:
        lf2.write('insert into documents_hashtags(hashtagid, documentid) values('+ str(idx_ht) + ' , ' + str(docid) + ');\n')
    idx_ht += 1

idx_at = 0
for key in attags:
    at_f.write('insert into attags(id, attag) values('+ str(idx_at) + ' , \'' + key + '\');\n')
    for docid in attags[key]:
        lf3.write('insert into documents_attags(attagid, documentid) values('+ str(idx_at) + ' , ' + str(docid) + ');\n')
    idx_at += 1

idx_word = 0
idx_vocab = 0
for key in words:
    words_f.write('insert into words(id, word) values('+ str(idx_word) + ' , \'' + key + '\');\n')
    for elem in words[key]:
        vocab_f.write('insert into vocabulary(id, documentid, wordid, tf, count) values('+ str(idx_vocab) + ' , ' + str(elem['documentid']) + ' , ' + str(idx_word) + ' , ' + str(elem['tf']) + ' , ' + str(elem['count']) +');\n')
        for p in elem['pos']:
            lf4.write('insert into vocabulary_pos(vocabularyid, posid) values('+ str(idx_vocab) + ' , ' + str(pos_id[p]) + ');\n')
        idx_vocab += 1
    idx_word += 1

doc_f.close()
auth_f.close()
lf1.close()
hash_f.close()
lf2.close()
at_f.close()
lf3.close()
words_f.close()
vocab_f.close()
lf4.close()