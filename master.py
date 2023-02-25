from multiprocessing import Process
import os

from mapper import Mapper


class Master(Process):
    def __init__(self, nMappers, nReducers, filePaths, fileMaxSize):
        self.nMappers = nMappers
        self.nReducers = nReducers
        self.filePaths = filePaths
        self.fileMaxSize = fileMaxSize

        self.fileDirectory = "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/assets"

        self.preprocessing()
        pass

    #preprocessing
    def preprocessing(self):
        self.splitFiles()
        self.initializeMasters()
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

    def initializeMasters(self):
        mappers = []
        for i in range(self.nMappers):
            mappers.append(Mapper(i))
        pass

    def initializeReducers():
        pass

    def checkFailedMasters():
        pass

    def checkFailedReducers():
        pass

    def assignFilePartitionsToMapper():
        pass
