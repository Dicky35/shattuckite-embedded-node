from datahandler import queueset as QSet
import logging
import time
import sys


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def PollingRedisMessage(hPubsub):
    """轮询redis消息

    注意该消息队列将在单独的线程中执行

    :param hPubsub: redis pubsub对象
    :type hPubsub: obj
    :param queueSet: IPC消息队列集合
    """
    logger.info("start new thread to polling redis message")
    QFatal = QSet.requireQueue('QFatal')

    try:
        QRedis = QSet.requireQueue('QRedis')
    except QSet.QueueDoesNoeExist:
        QFatal.put(sys.exc_info())

    while True:
        # here the get_message() method using select systemcall, so it is very fast. just use this infinite loop to polling is ok
        message = hPubsub.get_message()
        if message is not None:
            QRedis.put(message)
            # be nice to system
            time.sleep(0.01)
    