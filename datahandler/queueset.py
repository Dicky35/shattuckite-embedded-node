
from queue import Queue

__queueSet={} 

class QueueDoesNoeExist(Exception):
    pass

def requireQueue(queueName:str)->Queue:
    queueObj = __queueSet.get(queueName,None)
    if queueObj is None:
        raise QueueDoesNoeExist()
    else:
        return queueObj

def setQueue(queueName,queueObj)->None:
    __queueSet[queueName] = queueObj

def setMultipleQueue(**kwargs)->None:
    for k in kwargs:
        v=kwargs[k]
        if not  isinstance(k,str):
            raise AttributeError("Queue set key must be str")

        if not  isinstance(v,Queue):
            raise AttributeError("Queue set  value must be Queue obj")

        __queueSet[k] =v
