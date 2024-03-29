from multiprocessing import Process
from utility import WorkerStatus, getFileName
from keyValueClient import KeyValueClient

class Mapper(Process):

    def __init__(self, id, file, statusKey, countOutputKey, fileOutputKey):
        super(Mapper, self).__init__()
        self.id = id
        self.state = WorkerStatus.IDLE.value
        self.keyValueClient = KeyValueClient()
        self.file = file
        self.mapperKey = statusKey
        self.countKey = countOutputKey
        self.fileKey = fileOutputKey

    def processFile(self):
        try:
            fileName = getFileName(self.file)
            content = ""

            countContent = self.keyValueClient.getKey(self.countKey)

            fileContent = self.keyValueClient.getKey(self.fileKey)

            countContent = (" "+ countContent) if countContent else ""
            fileContent = (" "+ fileContent) if fileContent else ""

            with open(self.file, 'r') as f:
                content = f.read()

            content = content.replace("\t", " ")
            content = content.replace("\r\n", " ")
            content = content.replace("\n", " ")

            content = content.split(" ")

            for word in content:
                if word:
                    countContent = countContent + f" {word} 1"
                    fileContent = fileContent + f" {word} {fileName}"

            self.keyValueClient.setKey(self.countKey, countContent[1:])
            self.keyValueClient.setKey(self.fileKey, fileContent[1:])
        except Exception as e:
            print(f"Exception in {self.id} mapper: {e}")
            self.keyValueClient.setKey(self.mapperKey, WorkerStatus.FAILED.value)

    def run(self):
        print(f"Running Mapper {self.id}")
        self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IN_PROGRESS.value)
        self.processFile()
        self.keyValueClient.setKey(self.mapperKey, WorkerStatus.IDLE.value)





