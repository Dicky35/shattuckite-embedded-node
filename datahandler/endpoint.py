from datahandler import queueset as QSet
from paho.mqtt.client import Client,MQTTMessage  
from common.deserializer import Deserializer
from common.execption import DataFormatError
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def onMQTTConnected(client:Client, data, flas, rc):
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
    
    # TODO: hardcoded Topic. need make it configuable later
    client.subscribe("testHome/rpc",qos=2)
    logger.info('successfully connect to remote mqtt broker')


def onMQTTDataReceived(client:Client, userdata, message:MQTTMessage):
    """接收到Mqtt消息后的回调.

    :param client: paho mqtt对象
    :type client: obj
    :param userdata: 用户自定义数据
    :type userdata: obj
    :param message: Mqtt broker转发的消息
    :type message: obj
    """
    QRPC = QSet.requireQueue('QRPC')
    try:
        msgObj = Deserializer(message.payload)
        QRPC.put(msgObj)
        logger.debug("get message {0}".format(msgObj))
    except DataFormatError:
        logger.error("mqtt message deserialize failed, raw:{0}".format(message.payload))


def MqttPublish(mqttClient):
    """用于发布Mqtt消息的线程

    扮演Sink的角色。该线程从QMqttPub队列中订阅消息,然后将获取的消息推送到服务器。 
    
    请参见 pollingRedisMessage / dataHandle
    
    :param mqttClient: paho mqtt client对象
    :type mqttClient: object
    """
    QMqttPub = QSet.requireQueue('QMqttPub')
    while True:
        message = QMqttPub.get()  # type:bytes
        # TODO: hardcoded mqtt topic name
        remoteTopic = 'testHome/data'

        logger.debug("publish message to remote topic {topic}, data:{data}".format(
            topic=remoteTopic, data=message))

        mqttClient.publish(remoteTopic, payload=message, qos=2, retain=False)
