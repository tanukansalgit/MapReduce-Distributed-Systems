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
        try:
            fileName = self.file.split("/")[-1]
            content = ""

            countKey = getMapperCountOutputKey(self.id)
            countContent = self.keyValueClient.getKey(countKey)
            
            fileKey = getMapperFileOutputKey(self.id)
            fileContent = self.keyValueClient.getKey(fileKey)

            countContent = countContent.decode() if countContent else ""
            fileContent = fileContent.decode() if fileContent else ""

            with open(self.file, 'r') as f:
                content = f.read()
            content = content.split(" ")

            for word in content:
                countContent = countContent + f" {word} 1"
                fileContent = fileContent + f" {word} {fileName}"

            self.keyValueClient.setKey(countKey, countContent)
            self.keyValueClient.setKey(fileKey, fileContent)
        except Exception as e:
            print(f"Exception in {self.id} mapper: {e}")
            self.keyValueClient.setKey(self.mapperKey, WorkerStatus.FAILED.value)

    def run(self):
        self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IN_PROGRESS.value)
        self.processFile()
        self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IDLE.value)





