import redis
import argparse
import yaml
import threading
import functools
import asyncio
import datetime
import random
from queue import Queue
from common.serializer import Serializer


parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', type=argparse.FileType('r'),
                    help="shattuckite datahandler config file", required=True)

gMQ = Queue()


async def iamSensor(sensorId):
    """yes, indeed, i am a sensor
    """
    while True:
        await asyncio.sleep(random.randint(2,3)+random.random())
        print("sensorID:{id} generate Message".format(id=sensorId))
        gMQ.put(
            {
                "type": "sensor",
                "patload": {
                    "timestamp": datetime.datetime.utcnow().timestamp(),
                    "value": "{0:.2f}".format(random.randint(15, 40)+random.random()),
                    "sensorId": sensorId
                }
            }
        )


def redisPub(p):
    while True:
        m = gMQ.get()
        p.publish('data', Serializer(m))

async def setup():
    await asyncio.gather(* [asyncio.create_task(iamSensor(i)) for i in range(1,5)])

if __name__ == "__main__":
    args = parser.parse_args()
    conf = yaml.safe_load(args.config)

    redisClient = redis.Redis(
        host=conf['connection']['redis']['address'], port=conf['connection']['redis']['port'])

    hRedisPubSub = redisClient.pubsub()

    h=threading.Thread(target=functools.partial(redisPub,redisClient))
    h.start()
    loop  = asyncio.get_event_loop()
    asyncio.run(setup())
