from datetime import datetime
from elasticsearch import Elasticsearch
from numpy import *
import numpy
import time
import csv


S_ES_LIMIT = '10000'
S_GRANULARITY = '100'

# OBLIGATOIRE
S_INDEX = 'twitter_fdl_20152016'
S_DOCTYPE = 'tweet'
N_CSV_NUMBER = 2


# FILE
F_DATA_TO = "../Results/tweets_MABED.csv"
F_DATA_FROM = "../Results/data_MABED.csv"

#test
# OPTIONS
# TIMESTAMP EN MS !
S_TIMESTAMP_BEGIN = ''
S_TIMESTAMP_END = ''

# 1 = ES
N_OPTION = 1

class cHandleEs:
    def __init__(self):
        self.sCluster = ''

#    def __init__(self, sName):
#        self.sCluster = sName

    def connectionToEs(self):
        if len(self.sCluster):
            es = Elasticsearch(cluster=self.sCluster)
        else:
            es = Elasticsearch()
        return es

class cBatchId:
    def __init__(self, xEs, sIndexName, sDocTypeName, tKeyWords, sDateBegin, sDateEnd):
        self.xEs = xEs
        self.sIndexName = sIndexName
        self.sDocTypeName = sDocTypeName
        self.tKeyWords = tKeyWords
        self.sDateBegin = sDateBegin + '000'
        self.sDateEnd = sDateEnd + '000'
        self.nCurrentSize = 1
        self.nIndexSize = int(self.xEs.count(index = self.sIndexName)['count'])
        self.xIdPack = {}


        sKeyWords = '"'
        for word in tKeyWords:
            sKeyWords += word
            sKeyWords += ' '
        sKeyWords = sKeyWords[:-1]
        sKeyWords += '"'

        xFile = open(F_DATA_TO, "w", encoding='utf16')

        lFields = ['id_str']

        if self.sDateBegin and self.sDateEnd:

            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                  body={"query":{"bool":
                                                     {"must":[
                                                     {"match":
                                                          {"text":
                                                               {"query": sKeyWords, "operator": "or"}}},
                                                    {'range': {'timestamp_ms': {'gte': self.sDateBegin,
                                                                                     'lte': self.sDateEnd}}}
                                                     ]}}})


            self.nIndexSize = int(xResponse['hits']['total'])

            print("%d documents found" % xResponse['hits']['total'])

        elif self.sDateBegin:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'filter': {
                                            'and': [
                                                {'range': {'timestamp_ms': {'gte': S_TIMESTAMP_BEGIN}}},
                                                {"match": {"content": sKeyWords}}]}})
            print('if 2')
        elif self.sDateEnd:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'filter': {
                                            'and': [
                                                {'range': {'timestamp_ms': {'lte': S_TIMESTAMP_END}}},
                                                {"match": {"content": sKeyWords}}]}})
            print('if 3')
        else:
            xResponse = self.xEs.search(index=self.sIndexName, doc_type=self.sDocTypeName, scroll='10m',
                                        sort=['timestamp_ms:asc'], _source=lFields, stored_fields=lFields,
                                        body={'query': {"match": {"content": sKeyWords}}})
            print('if 4')

        self.sScroll = xResponse['_scroll_id']
        nCmpt = 0
        for hit in xResponse['hits']['hits']:
            #self.xIdPack.update({hit['_source']['id_str']:1})
            print(hit['_source']['text'])
            xFile.write(hit['_source']['id_str'] + '; ')
            xFile.write(hit['_source']['timestamp_ms'] + '; ')
            xFile.write(hit['_source']['text'] + '\n')
            self.nCurrentSize += 1
            nCmpt += 1

        #self.nIndexSize = int(self.xEs.count(index=self.sIndexName)['count'])
        print('Taille index : ' + str(self.nIndexSize))

        nCmpt += 1

        while (nCmpt < self.nIndexSize):
            try:
                nCmpt -= 1
                xResponse = self.xEs.scroll(scroll_id = self.sScroll, scroll ='10s')
                self.sScroll = xResponse['_scroll_id']
                for hit in xResponse['hits']['hits']:
                    #self.xIdPack.update({hit['_source']['id_str']:1})
                    test = (hit['_source']['text'])
                    xFile.write(hit['_source']['id_str'] + '; ')
                    xFile.write(hit['_source']['timestamp_ms'] + '; ')
                    xFile.write(hit['_source']['text'] + '\n')
                    self.nCurrentSize += 1
                    nCmpt += 1
                nCmpt += 1
            except e:
                break
        xFile.close()
        print ('FIN DE LA REQUETE ES')

    def getPack(self):
        return self.xIdPack


def extraction():
    xCsvFile = open(F_DATA_FROM, "r")
    nLine = 1
    for line in xCsvFile:
        if nLine == N_CSV_NUMBER:
            sDateBegin = line.split('; ')[0]
            sDateEnd = line.split('; ')[1]
            sKeyWords = line.split(';')[2][1:]
            tKeyWords = sKeyWords.split(', ')
            nLine += 1
        else:
            nLine += 1

    xhandleES = cHandleEs()
    xEs = xhandleES.connectionToEs()
    xBatchId = cBatchId(xEs, S_INDEX, S_DOCTYPE, tKeyWords, sDateBegin, sDateEnd)


def main():
    extraction()

if __name__ == '__main__':
    main()