from enum import Enum

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