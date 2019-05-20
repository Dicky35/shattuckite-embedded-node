from paho.mqtt import client
import redis
import sys
import asyncio
import argparse
import yaml
import logging
import threading
import time
import functools
from jsonschema import validate
from queue import Queue
from datahandler import queueset as QSet

from datahandler.datahandle import DataHandleFlow
from datahandler.redis import PollingRedisMessage
from datahandler.endpoint import onMQTTConnected,MqttPublish,onMQTTDataReceived


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
    mqttClient = client.Client(client_id=conf['node']['uid'])
    mqttClient.on_connect = onMQTTConnected
    mqttClient.on_message = onMQTTDataReceived

    logger.info("connecting to mqtt broker:{addr}:{port}".format(
        addr=conf['connection']['mqtt']['address'], port=conf['connection']['mqtt']['port']))

    mqttClient.connect(host=conf['connection']['mqtt']['address'],
                       port=conf['connection']['mqtt']['port'],
                       keepalive=conf['connection']['mqtt'].get('keepalive', 60))

    # redis connect was blocking
    logger.info("connecting to local redis kv database:{addr}:{port}".format(
        addr=conf['connection']['redis']['address'], port=conf['connection']['redis']['port']))

    redisClient = redis.Redis(
        host=conf['connection']['redis']['address'], port=conf['connection']['redis']['port'])

    logger.info("successfully connect to local redis".format(
        addr=conf['connection']['redis']['address'], port=conf['connection']['redis']['port']))

    hRedisPubSub = redisClient.pubsub()
    hRedisPubSub.subscribe('data')

    # this thread  to handle  mqtt network communication
    mqttClient.loop_start()
    QSet.setMultipleQueue(
        QFatal=Queue(),
        QRedis=Queue(),
        QMqttPub=Queue(),
    )

    QFatal = QSet.requireQueue('QFatal')

    # this thread to polling Redis message
    hPollingThread = threading.Thread(
        target=functools.partial(PollingRedisMessage, hRedisPubSub))
    hPollingThread.start()

    # this thread to filter data
    hFilterThread = threading.Thread(target=DataHandleFlow)
    hFilterThread.start()

    hMqttThread = threading.Thread(
        target=functools.partial(MqttPublish, mqttClient))
    hMqttThread.start()
    
    execInfo = QFatal.get()
    logging.critical("Fatal Error, exit whole program:{0}".format(execInfo))
