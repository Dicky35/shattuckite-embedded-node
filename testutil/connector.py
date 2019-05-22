from paho.mqtt import client
import logging
import sys
import threading
import functools
import queue

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


_mqttClient = None
_QMqttMsg=queue.Queue()


def onMQTTConnected(client, data, flas, rc,mutex):
    mutex.release()
    logger.info('successfully connect to remote mqtt broker')

def __onMQTTDataReceived(client, userdata, message):
    _QMqttMsg.put(message)
    logger.debug("get message {0}".format(message))


def getMqttClient():
    if _mqttClient is not None:
        return _mqttClient

    mutex = threading.Lock()
    mqttClient = client.Client(client_id="testScript")
    mqttClient.on_connect = functools.partial(onMQTTConnected,mutex = mutex)
    mqttClient.on_message = __onMQTTDataReceived
    #TODO:Hard Coded Server address, please change to use arg defined later
    mutex.acquire()
    logger.debug("connect to mqtt")
    mqttClient.connect('ali.cnworkshop.xyz', 20000, 60) 
    mqttClient.loop_start()
    mutex.acquire()
    return mqttClient

def getMqttMsgQueue():
    return _QMqttMsg

