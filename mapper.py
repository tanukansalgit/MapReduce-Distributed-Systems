from multiprocessing import Process
from utility import WorkerStatus, getMapperCountOutputKey, getMapperFileOutputKey, getMapperStatusKey
from keyValueClient import KeyValueClient

class Mapper(Process):

    def __init__(self, id, file):
        super(Mapper, self).__init__()
        self.id = id
        self.state = WorkerStatus.IDLE.value
        self.keyValueClient = KeyValueClient()
        self.file = file
        self.mapperKey = getMapperStatusKey(id)

    def processFile(self):
        print("in process fil fucntion")
        try:
            fileName = self.file.split("/")[-1]
            content = ""
            countKey = getMapperCountOutputKey(self.id)
            countContent = self.keyValueClient.getKey(countKey).decode() or ""
            fileKey = getMapperFileOutputKey(self.id)

            fileContent = self.keyValueClient.getKey(fileKey).decode() or ""

            print('countContent===', countContent)
            print('fileContent===', fileContent)

            with open(self.file, 'r') as f:
                content = f.read()
            content = content.split(" ")

            for word in content:
                countContent = countContent + f" {word} 1"
                fileContent = fileContent + f" {word} {fileName}"

            print('countContent===', countContent)
            print('fileContent===', fileContent)
            self.keyValueClient.setKey(countKey, countContent)
            self.keyValueClient.setKey(fileKey, fileContent)
            self.keyValueClient.setKey(self.mapperKey, WorkerStatus.COMPLETED.value)
        except:
            self.keyValueClient.setKey(self.mapperKey, WorkerStatus.FAILED.value)

    def run(self):
        #state chnge
        print('Running mapper run function', self.id)
        print('herer1')
        result1 = self.keyValueClient.getKey(self.mapperKey)
        print('get result==', result1)
        result= self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IN_PROGRESS.value)
        print('hererer2', result)
        result1 = self.keyValueClient.getKey(self.mapperKey)
        print('get result==', result1)
        self.processFile()
        # self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IDLE.value)





