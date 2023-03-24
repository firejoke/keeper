# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2022/6/1 16:00
from public_def import CONF, RpcClient, SRkvLocalSock


class API(object):
    def __init__(self):
        conf = CONF.get(__package__)
        self.rpc_client = RpcClient(heartbeat=conf.get("heartbeat"))
        self.rpc_client.connect("ipc://%s" % SRkvLocalSock)

    def info(self):
        return self.rpc_client.info()

    def create_kv(self, key, value):
        return self.rpc_client.create_kv(key, value)

    def update_kv(self, key, value):
        return self.rpc_client.update_kv(key, value)

    def delete_kv(self, key):
        return self.rpc_client.delete_kv(key)

    def get_kv(self, key, prefix=False):
        return self.rpc_client.get_kv(key, prefix=prefix)

    def close(self):
        self.rpc_client.close()
