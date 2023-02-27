from enum import Enum
import os

class WorkerStatus(Enum):
    IDLE = '0'
    IN_PROGRESS = '1'
    COMPLETED = '2'
    FAILED = '3'

def getMapperStatusKey(id):
    return f"{id}-mapper-status"

def getMapperCountOutputKey(id):
    return f"mapper-{id}-count-output"

def getMapperFileOutputKey(id):
    return f"mapper-{id}-file-output"

def getReducerStatusKey(id):
    return f"{id}-reducer-status"

def getReducerCountOutputKey(id):
    return f"reducer-{id}-count-output"

def getReducerFileOutputKey(id):
    return f"reducer-{id}-file-output"

def getFileName(filePath):
    return os.path.basename(filePath)
    pass
