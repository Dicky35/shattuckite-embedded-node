from datahandler import queueset as QSet
from datahandler.connector import getMqttclient
from common.serializer import Serializer
import os
import logging
import subprocess

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
threshold=5


class RPCExectuableNotFound(Exception):
    pass


def RpcExectuator(ExecPath):
    QRPC = QSet.requireQueue("QRPC")
    logger.info("start thread to handle RPC Request")
    while True:
        rpcReq = QRPC.get()
        try:
            threshold=int(rpcReq['Timeout'])
        except KeyError:
            threshold=5
        try:
            if(rpcReq['type'] == 'external'):
                __ExternalRPCExecuator(ExecPath, rpcReq)
        except KeyError:
            logger.error("rpc request format error")
        except RPCExectuableNotFound:
            logger.error("rpc exectuable not found")


def __ExternalRPCExecuator(ExecPath, rpcReq):
    fullName = "{0}/{1}".format(ExecPath, rpcReq['name'])
    if not __isExternelExecExist(fullName):
        logger.error("")
        raise RPCExectuableNotFound
    else:
        # type:subprocess.Popen
        args=[fullName,]
        for kv in rpcReq['parameter']:
            args.append(kv['key'])
            args.append(kv['value'])
        print(args)

        with subprocess.Popen(args=args, stderr=subprocess.PIPE, stdout=subprocess.PIPE ) as proc:
            # proc: subprocess.Popen
            outs, errs = proc.communicate(timeout=threshold)
            if proc.returncode != 0:  # RPC return errorCode
                __RPCResponse({
                    "id": rpcReq['id'],
                    "status": 'error',
                    "payload": str(errs, 'utf8')
                })
            else:
                __RPCResponse({
                    "id": rpcReq['id'],
                    "status": 'success',
                    "payload": str(outs, 'utf8')
                })
            logger.info("exectute {0} exit({1}) stdout:{2} stderr:{3}".format(
                fullName, proc.returncode, outs, errs))


def __isExternelExecExist(fullName):
    return os.path.isfile(fullName)


def __RPCResponse(resObj):
    mqttClient = getMqttclient()
    # TODO: Harcoded  topic name
    mqttClient.publish(topic="testHome/rpcres", qos=2, payload=Serializer(resObj))
