from master import Master

class MapReduce:
    def __init__(self, mappers, reducers, filePaths):
        self.nMappers = mappers
        self.nReducers = reducers
        self.filePaths = filePaths
        self.fileMaxSize = 100
        pass


    def initialiseMaster(self):
        master = Master(self.nMappers, self.nReducers, self.filePaths, self.fileMaxSize)
        pass


if __name__ == "__main__":
    mappers = 10
    reducers = 10
    filePaths = ["/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/input/file1.txt",
    "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/input/file2.txt",
    "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/input/file3.txt",
    "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/input/file4.txt",
    "/Users/tanukansal/Documents/distributedSystems/MapReduce-Distributed-Systems/input/file5.txt"
    ]

    mapReduce = MapReduce(mappers, reducers, filePaths)
    mapReduce.initialiseMaster()
