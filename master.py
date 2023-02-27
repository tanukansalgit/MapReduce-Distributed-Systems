from multiprocessing import Process
import os
from multiprocessing import Queue
import json

from mapper import Mapper
from reducer import Reducer
from keyValueClient import KeyValueClient
from utility import WorkerStatus, getMapperStatusKey, getMapperFileOutputKey, getMapperCountOutputKey, getReducerStatusKey, getReducerFileOutputKey, getReducerCountOutputKey, getFileName

class Master(Process):
    def __init__(self, nMappers, nReducers, filePaths, fileMaxSize, outputCountFile, outputInvertedIndexFile):
        super(Master, self).__init__()
        self.nMappers = nMappers
        self.nReducers = nReducers
        self.filePaths = filePaths
        self.fileMaxSize = fileMaxSize

        self.kvCountData = ""
        self.kvFileData = ""

        self.reducers = []
        self.idleReducers = 0
        self.availableReducers = set()
        self.reducerJobs = {}
        self.reducerCountOutputKeys = set()
        self.reducerFileOutputKeys = set()

        self.mappers = []
        self.idleMappers = 0
        self.availableMapperQueue = Queue()
        self.availableMappers = set()
        self.mapperJobs = {}
        self.mapperCountOutputKeys = set()
        self.mapperFileOutputKeys = set()

        self.keyValueClient = KeyValueClient()

        self.fileDirectory = "assets"
        self.outputCountFile = outputCountFile
        self.outputInvertedIndexFile = outputInvertedIndexFile
        pass

    #preprocessing
    def preprocessing(self):
        self.cleanKeyValue()
        self.splitFiles()
        self.initializeMappers()
        self.assignFilePartitionsToMapper()

        while(self.idleMappers != self.nMappers):
            self.checkForMappersStatus()

        self.initializeReducers()
        while(self.idleReducers != self.nReducers):
            self.checkForReducerStatus()
        self.processOutput()

    '''
    split files into equal size chunks, and output to a local folder
    Alternative, TODO:: to store start and end pointer for each chunk,
    to reduce overhead of making new files for each chunk
     '''
    def splitFiles(self):
        maxSize = self.fileMaxSize
        target = self.fileDirectory
        filePaths = []

        if not os.path.exists(target):
            os.makedirs(target)

        for inputFile in self.filePaths:
            fileName = getFileName(inputFile)

            with open(inputFile, 'rb') as file:
                startPointer = 0
                chunkNumber = 0

                while True:
                    file.seek(startPointer)
                    data = file.read(maxSize)
                    if not data:
                        break

                    lastSpace = data.rfind(b' ')
                    if lastSpace != -1:
                        newStart = file.tell()
                        while(1):
                            file.seek(newStart)
                            d = file.read(maxSize)
                            if not d:
                                break
                            spaceIndex = d.find(b" ")
                            if spaceIndex != -1:
                                data = data+d[:spaceIndex+1]
                                newStart = newStart + spaceIndex+1
                                break
                            else:
                                data = data + d
                                newStart = file.tell()
                        startPointer = newStart
                    else:
                        startPointer = file.tell()

                    newFile = os.path.join(target, f'{chunkNumber}-{fileName}')
                    with open(newFile, 'wb') as newF:
                        newF.write(data)
                    filePaths.append(newFile)
                    chunkNumber = chunkNumber + 1

            self.filePaths = filePaths

    def cleanKeyValue(self):
        self.keyValueClient.delete()
        pass

    def initializeMappers(self):
        for i in range(self.nMappers):
            self.availableMapperQueue.put(i)
            self.availableMappers.add(i)
            key = getMapperStatusKey(i)
            value = WorkerStatus.IDLE.value
            self.keyValueClient.setKey(key, value)

        self.idleMappers = self.nMappers

    def createMapper(self, id, file):
        countOutputKey = getMapperCountOutputKey(id)
        fileOutputKey = getMapperFileOutputKey(id)

        self.mapperCountOutputKeys.add(countOutputKey)
        self.mapperFileOutputKeys.add(fileOutputKey)

        return Mapper(id,
        file,
        getMapperStatusKey(id),
        countOutputKey,
        fileOutputKey)

    def assignFilePartitionsToMapper(self):
        totalFiles = len(self.filePaths)

        i = 0
        while(i<totalFiles):
            file = self.filePaths[i]
            if self.idleMappers:
                self.idleMappers = self.idleMappers - 1
                mapperId = self.availableMapperQueue.get()
                mapper = self.createMapper(mapperId,file)
                self.availableMappers.remove(mapperId)
                mapper.start()
                self.mapperJobs[mapperId] = file
                i = i+1
            else:
                while(not self.idleMappers):
                    self.checkForMappersStatus()

    def checkForMappersStatus(self):
        for i in range(self.nMappers):
            kv = self.keyValueClient.getKey(getMapperStatusKey(i))
            if kv and kv == WorkerStatus.IDLE.value and i not in self.availableMappers:
                self.availableMapperQueue.put(i)
                self.availableMappers.add(i)
                self.idleMappers = self.idleMappers + 1

    def initializeReducers(self):
        for i in range(self.nReducers):
            reducer = self.createReducer(i)
            reducer.start()
            reducer.join()
        self.idleReducers = self.nReducers
        pass

    def createReducer(self, id):
        countOutputKey = getReducerCountOutputKey(id)
        fileOutputKey = getReducerFileOutputKey(id)
        reducerStatusKey = getReducerStatusKey(id)

        self.reducerCountOutputKeys.add(countOutputKey)
        self.reducerFileOutputKeys.add(fileOutputKey)

        self.keyValueClient.setKey(reducerStatusKey, WorkerStatus.IDLE.value)

        return Reducer(id,
        self.nReducers,
        reducerStatusKey,
        countOutputKey,
        fileOutputKey,
        self.mapperFileOutputKeys,
        self.mapperCountOutputKeys)

    def checkFailedMappers(self):
        pass

    def checkFailedReducers(self):
        pass

    def checkForReducerStatus(self):
        for i in range(self.nReducers):
            kv = self.keyValueClient.getKey(getReducerStatusKey(i))
            if kv and kv == WorkerStatus.IDLE.value and i not in self.availableMappers:
                self.availableMappers.add(i)
                self.idleReducers = self.idleReducers + 1

    def processOutput(self):

        result = {}

        for key in self.reducerCountOutputKeys:
            value = self.keyValueClient.getKey(key)
            value = json.loads(value)

            for k in value:
                self.kvCountData = f"{self.kvCountData} {k} {value[k]}\n"

        for key in self.reducerFileOutputKeys:
            value = self.keyValueClient.getKey(key)
            value = json.loads(value)

            for k in value:
                self.kvFileData = f"{self.kvFileData} {k} {value[k]}\n"


        with open(self.outputCountFile, 'w', encoding='utf-8') as filename:
            filename.write(self.kvCountData)
            filename.close()

        with open(self.outputInvertedIndexFile, 'w', encoding='utf-8') as filename:
            filename.write(self.kvFileData)
            filename.close()

        pass

    def run(self):
        self.preprocessing()




