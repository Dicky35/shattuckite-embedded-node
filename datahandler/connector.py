from datahandler.endpoint import onMQTTConnected, MqttPublish, onMQTTDataReceived
from paho.mqtt import client
import logging
import functools
import os
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

_mqttClient = None
_hRedisPubsub = None


def getRedisClient(conf=None):
    global _hRedisPubsub
    if _hRedisPubsub is not None:
        return _hRedisPubsub

    # redis connect was blocking
    logger.info("connecting to local redis kv database:{addr}:{port}".format(
        addr=conf['connection']['redis']['address'], port=conf['connection']['redis']['port']))

    redisClient = redis.Redis(
        host=conf['connection']['redis']['address'], port=conf['connection']['redis']['port'])

    logger.info("successfully connect to local redis".format(
        addr=conf['connection']['redis']['address'], port=conf['connection']['redis']['port']))

    _hRedisPubsub = redisClient.pubsub()
    _hRedisPubsub.subscribe('data')
    return _hRedisPubsub


def getMqttclient(conf=None)->client.Client:
    global _mqttClient
    if _mqttClient is not None:
        return _mqttClient
    BASE_DIR = os.path.dirname(__file__)
    RPC_DIR = os.path.abspath(
        "{base}/{rpc}".format(base=BASE_DIR, rpc=conf['rpc']['execPath']))

    _mqttClient = client.Client(client_id=conf['node']['uid'])
    _mqttClient.on_connect = onMQTTConnected
    _mqttClient.on_message = functools.partial(
        onMQTTDataReceived, rpcPath=RPC_DIR)

    _mqttClient.loop_start()

    logger.info("connecting to mqtt broker:{addr}:{port}".format(
        addr=conf['connection']['mqtt']['address'], port=conf['connection']['mqtt']['port']))

    _mqttClient.connect(host=conf['connection']['mqtt']['address'],
                        port=conf['connection']['mqtt']['port'],
                        keepalive=conf['connection']['mqtt'].get('keepalive', 60))

    return _mqttClient
