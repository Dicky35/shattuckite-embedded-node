import sys
import argparse
import yaml
import logging
import threading
import time
import functools
from jsonschema import validate
from queue import Queue
from datahandler import queueset as QSet
import os

from datahandler.datahandle import DataHandleFlow
from datahandler.redis import PollingRedisMessage

from datahandler.connector import getMqttclient, getRedisClient
from datahandler.endpoint import MqttPublish
from datahandler.rpc import RpcExectuator


# logging setting
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# parser setting
parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', type=argparse.FileType('r'),
                    help="shattuckite datahandler config file", required=True)


def run():
    args = parser.parse_args()
    conf = yaml.safe_load(args.config)
    logger.info("system start")

    BASE_DIR = os.path.dirname(__file__)
    RPC_DIR = os.path.abspath(
        "{base}/{rpc}".format(base=BASE_DIR, rpc=conf['rpc']['execPath']))

    logger.info("run at {0}".format(BASE_DIR))
    logger.info("RPC exec path at {0}".format(RPC_DIR))

    hRedisPubSub = getRedisClient(conf)
    print(hRedisPubSub)
    mqttClient = getMqttclient(conf)
    print(mqttClient)

    # this thread  to handle  mqtt network communication
    QSet.setMultipleQueue(
        QFatal=Queue(),
        QRedis=Queue(),
        QMqttPub=Queue(),
        QRPC=Queue(),
    )

    QFatal = QSet.requireQueue('QFatal')

    # this thread to polling Redis message
    hPollingThread = threading.Thread(
        target=functools.partial(PollingRedisMessage, hRedisPubSub))
    hPollingThread.start()

    # this thread to filter data
    hFilterThread = threading.Thread(target=DataHandleFlow)
    hFilterThread.start()

    # this thread to  publish data to mqtt
    hMqttThread = threading.Thread(
        target=functools.partial(MqttPublish, mqttClient))
    hMqttThread.start()

    # this thread to get RPC Info
    hRpcThread = threading.Thread(
        target=functools.partial(RpcExectuator,RPC_DIR)
    )
    hRpcThread.start()

    execInfo = QFatal.get()
    logging.critical("Fatal Error, exit whole program:{0}".format(execInfo))
