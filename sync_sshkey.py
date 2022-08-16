# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/7/14 15:26
import signal
from logging import getLogger

import stat
import time

import os
import re

from public_def import (
    chmod, chown, common_text, get_host_ip, ipv4_pattern, read_hosts, local_cmd,
    RemoteError
)
from srkv.api import API


logger = getLogger(__name__)


def get_id_pub(user, pub_path):
    """
    :param user: str
    :param pub_path: str
    """
    interactives = [
        {'pattern': r'(?i)Overwrite', 'response': 'y\n'},
        {'pattern': r'(?i)Enter', 'response': '\n'},
    ]
    if not os.path.exists(pub_path):
        if user == "root":
            res = local_cmd('ssh-keygen -t rsa', interactives, pty=True)
        else:
            res = local_cmd(
                'su - %s -c"ssh-keygen -t rsa"' % user, interactives, pty=True
            )
        if re.search(r"(?i)error|failed", res[1]):
            logger.error("create vestack ssh_key failed: %s" % res[1])
            return None, 0
    with open(pub_path) as pf:
        return pf.read().strip(), os.path.getmtime(pub_path)


def get_rsa_known(hostname, host_ip):
    rsa_known = local_cmd("ssh-keyscan -t rsa %s,%s" % (hostname, host_ip))
    if re.search(r"(?i)error|failed", rsa_known[1]):
        logger.error("keyscan faield: %s" % rsa_known[1])
        return None
    else:
        rsa_known = rsa_known[0]
    return rsa_known.strip()


def get_known_hosts(known_path):
    kd = dict()
    if os.path.exists(known_path):
        with open(known_path) as knf:
            for hs in knf.readlines():
                logger.info(hs)
                _k = hs.strip().split()
                if _k:
                    logger.info(_k[0])
                    if "," in _k[0]:
                        for e in _k[0].split(","):
                            if re.search(ipv4_pattern, e):
                                kd[e.strip()] = hs.strip()
                    _ip = re.match(ipv4_pattern, _k[0].strip())
                    if _ip and _ip == _k[0].strip():
                        kd[_k[0].strip()] = hs.strip()
        return kd, os.path.getmtime(known_path)
    return kd, 0


def write_known_hosts(known, known_path, owner, group):
    khs = "\n".join(known.values())
    khs += "\n"
    with open(known_path, "w") as khf:
        khf.write(khs)
    chmod(
        known_path,
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
    )
    chown(known_path, owner, group)


def get_auth_keys(keys_path):
    auth_keys = dict()
    if os.path.exists(keys_path):
        with open(keys_path) as af:
            for key in af.readlines():
                k = key.strip().split("@")
                if k:
                    auth_keys[k[-1].strip()] = key.strip()
        return auth_keys, os.path.getmtime(keys_path)
    return auth_keys, 0


def write_auth_keys(auth_keys, auth_path, owner, group):
    keys = "\n".join(auth_keys.values())
    keys += "\n"
    with open(auth_path, "w") as f:
        f.write(keys)
    chmod(
        auth_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IRGRP
    )
    chown(auth_path, owner, group)


def get_all_auth_keys(v_keys_path, r_keys_path):
    v_auth_keys, v_keys_mtime = get_auth_keys(v_keys_path)
    r_auth_keys, r_keys_mtime = get_auth_keys(r_keys_path)
    auth_keys = {
        host: {
            "vestack": v_auth_keys.get(host, ''),
            "root": r_auth_keys.get(host, '')
        } for host in set(v_auth_keys.keys() + r_auth_keys.keys())
    }
    return auth_keys, v_keys_mtime, r_keys_mtime


def write_all_auth_keys(all_auth_keys, v_keys_path, r_keys_path):
    v_auth_keys = dict()
    r_auth_keys = dict()
    for _ip, keys in all_auth_keys.items():
        vk = keys.get("vestack")
        rk = keys.get("root")
        if vk:
            v_auth_keys[_ip] = vk
        if rk:
            r_auth_keys[_ip] = rk
    write_auth_keys(v_auth_keys, v_keys_path, "vestack", "vestack")
    write_auth_keys(r_auth_keys, r_keys_path, "root", "root")


def proc():
    """
    sync ssh key for srkv
    """
    my_pid = os.getpid()
    sr = API()
    # etcd_port = CONF.get("etcd", dict()).get("client_port", 22379)
    # etcd_users = etcd_get_users(etcd_port)
    # if etcd_users == 401:
    #     etcd_users = etcd_get_users(etcd_port, "root", "vestack")
    # if etcd_users == 500:
    #     logger.error("not found etcd users")
    #     return
    # if isinstance(etcd_users, list):
    #     if "root" not in etcd_users:
    #         add_res = etcd_add_user(etcd_port, "root", "vestack")
    #         if add_res == 500:
    #             return
    #         elif add_res == 401:
    #             logger.error("root password not valid")
    #             return

    def exit_proc(signum=None, frame=None):
        if os.getpid() == my_pid:
            logger.warning("%s exit" % __name__)
            sr.close()
        return

    signal.signal(signal.SIGINT, exit_proc)

    v_pub_path = "/opt/vestack/.ssh/id_rsa.pub"
    r_pub_path = "/root/.ssh/id_rsa.pub"
    v_known_path = "/opt/vestack/.ssh/known_hosts"
    r_known_path = "/root/.ssh/known_hosts"
    v_keys_path = "/opt/vestack/.ssh/authorized_keys"
    r_keys_path = "/root/.ssh/authorized_keys"
    id_pub = dict()
    hostname = os.uname()[1]
    hosts, hosts_mtime = read_hosts()
    host_ip = get_host_ip(hostname, hosts)
    if not host_ip:
        logger.error("not found host ip from hosts.")
        return
    logger.info("host ip: %s, hosts mtime: %s.", host_ip, hosts_mtime)
    r_pub, r_pub_mtime = get_id_pub("root", r_pub_path)
    if not r_pub:
        return
    logger.info("r pub: %s, r pub mtime: %s." % (r_pub, r_pub_mtime))
    v_pub, v_pub_mtime = get_id_pub("vestack", v_pub_path)
    if not v_pub:
        return
    logger.info("v pub: %s, v pub mtime: %s." % (v_pub, v_pub_mtime))
    id_pub.update(vestack=v_pub, root=r_pub)
    logger.info("id pub: %s." % id_pub)
    auth_keys, v_keys_mtime, r_keys_mtime = get_all_auth_keys(
        v_keys_path, r_keys_path
    )
    logger.info("v_keys_mtime: %s." % v_keys_mtime)
    logger.info("r_keys_mtime: %s." % r_keys_mtime)
    rsa_known = get_rsa_known(hostname, host_ip)
    if not rsa_known:
        return
    logger.info("rsa_know: %s." % rsa_known)
    v_known_hosts, v_known_mtime = get_known_hosts(v_known_path)
    logger.info(
        "v know hosts: %s, v known mtime: %s." % (v_known_hosts, v_known_mtime)
    )
    r_known_hosts, r_known_mtime = get_known_hosts(r_known_path)
    logger.info(
        "r know hosts: %s, r known mtime: %s." % (r_known_hosts, r_known_mtime)
    )
    while 1:
        if os.uname()[1] != hostname:
            hosts, hosts_mtime = read_hosts()
            host_ip = get_host_ip(hostname, hosts)
            if not host_ip:
                return
            rsa_known = get_rsa_known(hostname, host_ip)
            if not rsa_known:
                return
        try:
            logger.debug("get ssh_keys from srkv")
            ssh_keys = sr.get_kv("ssh_keys")
            logger.info("ssh_keys: %s" % ssh_keys)
        except RemoteError as e:
            logger.warning(e)
            ssh_keys = dict()
            logger.info("save ssh_keys: %s" % ssh_keys)
            sr.create_kv("ssh_keys", ssh_keys)
        try:
            logger.debug("get known_hosts from srkv")
            known_hosts = sr.get_kv("known_hosts")
            logger.info("known_hosts: %s" % known_hosts)
        except RemoteError as e:
            logger.warning(e)
            known_hosts = dict()
            logger.info("save known_hosts: %s" % known_hosts)
            sr.create_kv("known_hosts", known_hosts)
        # check and set ssh-keygen
        if os.path.getmtime(v_pub_path) > v_pub_mtime or \
                os.path.getmtime(r_pub_path) > r_pub_mtime:
            v_pub, v_pub_mtime = get_id_pub("vestack", v_pub_path)
            r_pub, r_pub_mtime = get_id_pub("root", r_pub_path)
            if not v_pub or not r_pub:
                return
            id_pub.update(vestack=v_pub, root=r_pub)

        if ssh_keys.get(hostname, {}) != id_pub:
            ssh_keys[hostname] = id_pub
            logger.info("update ssh_keys: %s" % ssh_keys)
            sr.update_kv("ssh_keys", ssh_keys)
        # check and set ssh-keyscan
        if os.path.getmtime("/etc/hosts") > hosts_mtime:
            hosts, hosts_mtime = read_hosts()
            host_ip = get_host_ip(hostname, hosts)
            rsa_known = get_rsa_known(hostname, host_ip)
        if known_hosts.get(host_ip) != rsa_known:
            known_hosts[host_ip] = rsa_known
            logger.info("update known_hosts: %s" % known_hosts)
            sr.update_kv("known_hosts", known_hosts)
        # check and change authorized keys
        if (
                os.path.exists(v_keys_path) and
                os.path.getmtime(v_keys_path) > v_keys_mtime
        ) or (
                os.path.exists(r_keys_path) and
                os.path.getmtime(r_keys_path) > r_keys_mtime
        ):
            auth_keys, v_keys_mtime, r_keys_mtime = get_all_auth_keys(
                v_keys_path, r_keys_path
            )
            logger.info("v_keys_mtime: %s." % v_keys_mtime)
            logger.info("r_keys_mtime: %s." % r_keys_mtime)
        ssh_keys = sr.get_kv("ssh_keys")
        _change = 0
        for _hs, ks in ssh_keys.items():
            if _hs not in auth_keys or auth_keys[_hs] != ks:
                _change = 1
                auth_keys[_hs] = ks
                logger.info("auth_keys add: %s." % ks)
        if _change:
            write_all_auth_keys(
                auth_keys, v_keys_path, r_keys_path
            )
        # check and change known_hosts
        if os.path.exists(v_known_path) and \
                os.path.getmtime(v_known_path) > v_known_mtime:
            v_known_hosts, v_known_mtime = get_known_hosts(v_known_path)
            logger.info(
                "v know hosts: %s, v known mtime: %s." %
                (v_known_hosts, v_known_mtime)
            )
        if os.path.exists(r_known_path) and \
                os.path.getmtime(r_known_path) > r_known_mtime:
            r_known_hosts, r_known_mtime = get_known_hosts(r_known_path)
            logger.info(
                "r know hosts: %s, r known mtime: %s." %
                (r_known_hosts, r_known_mtime)
            )
        _v_change = 0
        _r_change = 0
        for _ip, h in known_hosts.items():
            if _ip not in v_known_hosts or v_known_hosts[_ip] != h:
                _v_change = 1
                logger.info("v known_hosts add: %s." % h)
                v_known_hosts[_ip] = h
            if _ip not in r_known_hosts or r_known_hosts[_ip] != h:
                _r_change = 1
                r_known_hosts[_ip] = h
                logger.info("r known_hosts add: %s." % h)
        if _v_change:
            write_known_hosts(v_known_hosts, v_known_path, "vestack", "vestack")
        if _r_change:
            write_known_hosts(r_known_hosts, r_known_path, "root", "root")
        time.sleep(3)
