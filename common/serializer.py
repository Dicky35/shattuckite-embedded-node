import json

def Serializer(data:dict)->str:
    return json.dumps(data)
