from testutil.connector import getMqttClient,getMqttMsgQueue
from common.serializer import Serializer
from paho.mqtt.client import MQTTMessage
import time

client=getMqttClient()
client.subscribe('testHome/rpcres',qos=2)
queue = getMqttMsgQueue()

#TODO: hardcoded Topic name

HelloWorldRPCReq={
    "type":"external",
    "name":"helloworld",
    "id":"testhash",
    "parameter":[
        {
            "key":"ActuatorID",
            "value":"50A1",
        },
        {
            "key":"State",
            "value":"1",
        }
    ]
}

ParameterizedRPCRea={
    "type":"external",
    "name":"paramEcho",
    "id":"testhash",
    "parameter":[
        {
            "key":"tParam1",
            "value":"v1",
        },
        {
            "key":"tParam2",
            "value":"v2",
        }
    ]
}

while True:
    client.publish('testHome/rpc',payload=Serializer(HelloWorldRPCReq),qos=2)
    msg=queue.get() #type: MQTTMessage
    print(msg.payload)

    client.publish('testHome/rpc',payload=Serializer(ParameterizedRPCRea),qos=2)
    msg=queue.get() #type: MQTTMessage
    print(msg.payload)
    time.sleep(2)

