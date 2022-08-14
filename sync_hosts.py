# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/11/3 17:57
import json
import signal
from logging import getLogger

import time

import os

from public_def import common_text, get_host_ip, read_hosts
from srkv.api import API


logger = getLogger(__name__)


def write_hosts(hosts):
    hs = "\n".join([ht[1] for ht in hosts.values()])
    hs += "\n"
    with open("/etc/hosts", "w") as hf:
        hf.write(hs)


def proc():
    """
    sync /etc/hosts
    :return:
    """
    my_pid = os.getpid()
    sr = API()

    def exit_proc(signum=None, frame=None):
        if os.getpid() == my_pid:
            logger.warning("%s exit" % __name__)
            sr.close()
        return

    signal.signal(signal.SIGINT, exit_proc)

    hosts, hosts_mtime = read_hosts()
    logger.info(hosts)
    hostname = os.uname()[1]
    host_ip = get_host_ip(hostname, hosts)
    if not host_ip:
        logger.error("not found host ip from hosts.")
    logger.info(host_ip)
    while 1:
        if os.uname()[1] != hostname:
            hosts, hosts_mtime = read_hosts()
            host_ip = get_host_ip(hostname, hosts)
        try:
            logger.debug("get nodes from srkv")
            nodes = sr.get_kv("nodes")
            logger.info("nodes: %s" % nodes)
        except Exception as e:
            logger.warning(common_text(e))
            nodes = {host_ip: hosts[host_ip]}
            logger.info("save nodes: %s" % nodes)
            sr.create_kv("nodes", nodes)
        if nodes.get(host_ip, []) != hosts[host_ip]:
            nodes[host_ip] = hosts[host_ip]
            logger.info("update nodes: %s" % nodes)
            sr.update_kv("nodes", nodes)
        if os.path.getmtime("/etc/hosts") > hosts_mtime:
            hosts, hosts_mtime = read_hosts()
        nodes = sr.get_kv("nodes")
        _change = 0
        for hip, hs in nodes.items():
            if hip not in hosts or hosts[hip] != hs:
                _change = 1
                logger.info("hosts add %s." % hs)
                hosts[hip] = hs
        if _change:
            write_hosts(hosts)
        time.sleep(3)
