from multiprocessing import Process
from keyValueClient import KeyValueClient

class Reducer(Process):

    def __init__(self, id):
        super(Reducer, self).__init__()
        self.id = id
        self.keyValueClient = KeyValueClient
        pass

    def processFiles(self):

        pass

