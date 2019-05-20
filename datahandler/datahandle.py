from datahandler import queueset as QSet
from common.serializer import Serializer
from common.deserializer import Deserializer
from common.execption import DataFormatError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def DataHandleFlow():
    """数据分发处理模块

    扮演Flow的角色，用于对Redis消息进行清洗过滤与分发处理.
    """
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
