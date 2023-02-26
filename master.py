from multiprocessing import Process
import os
from multiprocessing import Queue

from mapper import Mapper
from keyValueClient import KeyValueClient
from utility import WorkerStatus, getMapperStatusKey, getMapperFileOutputKey, getMapperCountOutputKey
from keyValueClient import KeyValueClient

class Master(Process):
    def __init__(self, nMappers, nReducers, filePaths, fileMaxSize):
        super(Master, self).__init__()
        self.nMappers = nMappers
        self.nReducers = nReducers
        self.filePaths = filePaths
        self.fileMaxSize = fileMaxSize
        self.kvData = {}

        self.reducers = []
        self.idleReducers = 0
        self.availableReducerQueue = Queue()
        self.reducerJobs = {}

        self.mappers = []
        self.idleMappers = 0
        self.availableMapperQueue = Queue()
        self.mapperJobs = {}
        self.mapperCountOutputKeys = set()
        self.mapperFileOutputKeys = set()

        self.keyValueClient = KeyValueClient()

        self.fileDirectory = "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/assets"
        pass

    #preprocessing
    def preprocessing(self):
        self.splitFiles()
        self.initializeMappers()
        self.assignFilePartitionsToMapper()

        while(self.idleMappers != self.nMappers):
            self.checkForMappersStatus()

        self.initializeReducers()

        pass

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
            fileName = inputFile.split("/")[-1]

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

    def initializeMappers(self):
        for i in range(self.nMappers):
            self.availableMapperQueue.put(i)
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
                mapper.start()
                self.mapperJobs[mapperId] = file
                i = i+1
            else:
                while(not self.idleMappers):
                    self.checkForMappersStatus()

    def checkForMappersStatus(self):
        for i in range(self.nMappers):
            kv = self.keyValueClient.getKey(getMapperStatusKey(i))
            if kv and kv.decode() == WorkerStatus.IDLE.value :
                self.availableMapperQueue.put(i)
                self.idleMappers = self.idleMappers + 1


    def initializeReducers(self):
        pass

    def checkFailedMappers(self):
        pass

    def checkFailedReducers(self):
        pass

    def run(self):
        self.preprocessing()




