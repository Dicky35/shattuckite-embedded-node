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
from common.serializer import Serializer
from common.deserializer import Deserializer
from common.execption import DataFormatError

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


def onMQTTConnected(client, data, flas, rc):
    """Mqtt服务器连接成功回调函数

    :param client: paho mqtt对象
    :type client: obj
    :param data: 私有用户消息
    :type data: obj
    :param flas: Broker返回的连接标志
    :type flas: obj
    :param rc: 连接结果
    :type rc: Int
    """
    logger.info('successfully connect to remote mqtt broker')


def onMQTTDataReceived(client, userdata, message):
    """接收到Mqtt消息后的回调.

    :param client: paho mqtt对象
    :type client: obj
    :param userdata: 用户自定义数据
    :type userdata: obj
    :param message: Mqtt broker转发的消息
    :type message: obj
    """
    pass


def pollingRedisMessage(hPubsub):
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


def dataHandle():
    QRedis = QSet.requireQueue('QRedis')
    QMqttPub = QSet.requireQueue('QMqttPub')
    while True:
        message = QRedis.get()
        logger.debug('Get redis raw message {0}'.format(message))
        type = message.get('type', None)
        if type == 'subscribe':
            logger.info("redis subscribe successfully".format())
        elif type == 'message':
            try:
                bytes = message['data']
                data = Deserializer(bytes)
                logger.debug("Get channel data: {data}".format(data=data))
                if data.get('type', None) == 'sensor':
                    logger.debug(
                        "Get Sensor data package, forward to Mqtt channel")
                    QMqttPub.put(bytes)

            except DataFormatError:
                logger.error("can not deserialize channel data: {channel}-{data}".format(
                    channel=message['channel'], data=message['data']))


def MqttPublish(mqttClient):
    QMqttPub = QSet.requireQueue('QMqttPub')
    while True:
        message = QMqttPub.get()  # type:bytes
        # TODO: hardcoded mqtt topic name
        remoteTopic = 'testHome/data'

        logger.debug("publish message to remote topic {topic}, data:{data}".format(
            topic=remoteTopic, data=message))

        mqttClient.publish(remoteTopic, payload=message, qos=2, retain=False)


def run():
    args = parser.parse_args()
    conf = yaml.safe_load(args.config)
    logger.info("system start")
    mqttClient = client.Client(client_id=conf['node']['uid'])
    mqttClient.on_connect = onMQTTConnected
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
        target=functools.partial(pollingRedisMessage, hRedisPubSub))
    hPollingThread.start()

    # this thread to filter data
    hFilterThread = threading.Thread(target=dataHandle)
    hFilterThread.start()

    hMqttThread = threading.Thread(
        target=functools.partial(MqttPublish, mqttClient))
    hMqttThread.start()
    
    execInfo = QFatal.get()
    logging.critical("Fatal Error, exit whole program:{0}".format(execInfo))
