from multiprocessing import Process

class Mapper(Process):
    def __init__(self, id):
        super().__init__()
        self.id = id
        self.file = ""
