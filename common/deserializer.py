import json
from . import execption as execption

def Deserializer(payload:bytes)->dict:
    try:
        return json.loads(str(payload,'utf8'))
    except:
        raise execption.DataFormatError()
