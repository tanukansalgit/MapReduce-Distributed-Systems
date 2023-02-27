from multiprocessing import Process
from keyValueClient import KeyValueClient
from utility import WorkerStatus
import hashlib
import json

class Reducer(Process):

    def __init__(self, id, nReducers, reducerKey, reducerCountOuputKey, reducerFileOutputKey, fileKeys, countKeys):
        super(Reducer, self).__init__()
        self.id = id
        self.nReducers = nReducers
        self.keyValueClient = KeyValueClient()
        self.reducerKey = reducerKey
        self.reducerCountOuputKey = reducerCountOuputKey
        self.reducerFileOutputKey = reducerFileOutputKey
        self.fileOutputKeys = fileKeys
        self.countOutputKeys = countKeys

        self.resultCountKeys = {}
        self.resultFileKeys = {}
        pass

    def assignedKey(self, key):
        hashKey = key.encode("utf8")
        h = hashlib.sha256(hashKey)

        if int(h.hexdigest(), base=16) % self.nReducers != self.id:
            return False
        return True

    def processFiles(self):
        try:
            for key in self.countOutputKeys:
                valueString = self.keyValueClient.getKey(key)
                values = valueString.split(" ")

                length = len(values)

                for i in range(0,length,2):
                    val = values[i]
                    if val and self.assignedKey(val):
                        if val not in self.resultCountKeys:
                            self.resultCountKeys[val] = 0
                        self.resultCountKeys[val] = self.resultCountKeys[val] + 1

            for key in self.fileOutputKeys:
                valueString = self.keyValueClient.getKey(key)
                values = valueString.split(" ")

                length = len(values)

                for i in range(0,length,2):
                    word = values[i]
                    val = values[i+1] if i+1 < length else ""
                    fileName = val.split("-", 1)[1]
                    if self.assignedKey(word):
                        if word not in self.resultFileKeys:
                            self.resultFileKeys[word] = []
                        if fileName not in self.resultFileKeys[word]:
                            self.resultFileKeys[word].append(fileName)

            self.keyValueClient.setKey(self.reducerCountOuputKey, json.dumps(self.resultCountKeys))
            self.keyValueClient.setKey(self.reducerFileOutputKey, json.dumps(self.resultFileKeys))
        except:
            print('Exception in reducer')
            self.keyValueClient.setKey(self.reducerKey, WorkerStatus.FAILED.value)


    def run(self):
        self.keyValueClient.setKey(self.reducerKey, WorkerStatus.IN_PROGRESS.value)
        self.processFiles()
        self.keyValueClient.setKey(self.reducerKey, WorkerStatus.IDLE.value)
