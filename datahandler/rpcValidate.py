from jsonschema import validate
from rpcschema import schema
import json

def rpcValidate(rpcRequest):
    validate(instance = rpcRequest ,schema = schema["request"])

fakeRPCRequest={
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

if __name__ == "__main__":
    rpcValidate(fakeRPCRequest)
