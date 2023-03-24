# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/7/5 17:22
import signal
import sys
from logging import getLogger

import os
import re
from pprint import pformat
from traceback import format_exception

import time

import MySQLdb
from public_def import (
    CONF, Config, OS_SERIES, check_port, query_process, NoOptionError,
    NoSectionError, RemoteError,
)
from srkv.api import API


logger = getLogger(__name__)


def connect_mysql(user, password, port):
    try:
        conn = MySQLdb.connect(
            host="localhost", user=user, password=password, port=port,
            connect_timeout=10
        )
    except Exception:
        logger.error(
            "database connection failed:\n%s"
            % format_exception(*sys.exc_info())
        )
        conn = None
    return conn


def get_data_dir_stat(datadir):
    mtime = 0
    for _p in os.listdir(datadir):
        _p = os.path.join(datadir, _p)
        if os.path.isdir(_p):
            for _f in os.listdir(_p):
                _f = os.path.join(_p, _f)
                _mtime = os.path.getmtime(_f)
                if _mtime > mtime:
                    logger.debug("%s: %s" % (_f, _mtime))
                    mtime = _mtime
    return mtime


def get_grastate(datadir):
    try:
        with open(os.path.join(datadir, "grastate.dat")) as gf:
            bootstrap = re.search(r"safe_to_bootstrap:\s*(\d)",
                                  gf.read()).group(1)
            bootstrap = bool(int(bootstrap))
    except OSError:
        bootstrap = False
    return bootstrap


def check_bootstrap_proc():
    process_info = query_process("mysqld")
    if process_info:
        if "--wsrep-new-cluster" in " ".join(process_info["cmdline"]):
            return True
    return False


def check_mysql_online(port):
    number = 2
    t = 2
    chk_res = False
    while number > 0:
        chk_res, message = check_port(port)
        if message:
            logger.error(message)
        time.sleep(t)
        number -= 1
    return chk_res


def get_wsrep_status(conn):
    conn.query('SHOW STATUS LIKE "WSREP%"')
    status = conn.store_result()
    status = {
        var["Variable_name"]: var["Value"]
        for var in status.fetch_row(maxrows=0, how=1)
    }
    logger.debug("get wsrep status: %s" % status)
    return status


def get_wsrep_variables(conn):
    conn.query('SHOW GLOBAL VARIABLES LIKE "WSREP%"')
    variables = conn.store_result()
    variables = {
        var["Variable_name"]: var["Value"]
        for var in variables.fetch_row(maxrows=0, how=1)
    }
    logger.debug("get wsrep variables: %s" % variables)
    return variables


def reset_bootstrap(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """SET GLOBAL wsrep_provider_options=\"pc.bootstrap=true\""""
        )
        return True
    except Exception as e:
        logger.error(e)
        return False


def get_local_role(nodes, cluster_address, node_address, mysql_port):
    """
    在机器重启的时候
    在用srkv保存了各个节点的状态时，需要先决策出一个引导节点
    """
    two_node = len(cluster_address) == 2
    if two_node:
        logger.warning("This Galera cluster is a two-node insecure cluster.")
    my_name = "galera_%s" % node_address
    logger.debug("nodes: %s" % nodes)
    if len(nodes) != len(cluster_address):
        logger.error(
            "Information about all nodes in the cluster is not obtained, "
            "nodes: %s, cluster nodes: %s" %
            (nodes.keys(), cluster_address)
        )
    bootstrap = filter(lambda k: nodes[k].get("bootstrap"), nodes)
    if bootstrap:
        logger.info("bootstrap node: %s" % bootstrap)
    if nodes:
        last_update = max(nodes, key=lambda k: nodes[k].get("date", 0))
    else:
        last_update = ""
    if last_update:
        logger.info("last update node: %s" % last_update)
    online_nodes = filter(
        lambda ip: check_port(mysql_port, ip)[0],
        cluster_address
    )
    if my_name in bootstrap:
        if len(bootstrap) > 1:
            if my_name == last_update:
                return 1
            else:
                return 0
        else:
            if two_node:
                if node_address in online_nodes:
                    return 1
                else:
                    return 0
            return 1
    else:
        if not online_nodes:
            if two_node and my_name == last_update:
                return 1
            return 0
        else:
            if two_node and len(online_nodes) == 1 and node_address in online_nodes:
                return 1
        return 0


def proc():
    my_pid = os.getpid()
    sr = API()
    logger.info("review galera cluster")
    for mysqld in ("/usr/sbin/mysqld", "/usr/libexec/mysqld"):
        if os.path.exists(mysqld):
            break
    else:
        logger.error("not found mysqld")
        sys.exit(signal.SIGINT)

    mysql_conf = Config(allow_no_value=True)
    if OS_SERIES == 'redhat':
        _path = ('/etc/my.cnf.d/server.cnf', '/etc/my.cnf.d/mariadb-server.cnf')
    else:
        _path = ('/etc/mysql/mariadb.conf.d/50-server.cnf',)
    for p in _path:
        if os.path.exists(p):
            mysql_conf.read(p)
            break
    else:
        logger.error('The MySQL configuration file could not be found')
        sys.exit(signal.SIGINT)

    try:
        datadir = mysql_conf.get("mysqld", "datadir")
    except NoOptionError:
        datadir = "/var/lib/mysql"
    try:
        port = int(mysql_conf.get("mysqld", "port"))
    except NoOptionError:
        port = 3306
    try:
        node_name = mysql_conf.get("galera", "wsrep_node_name")
        node_address = mysql_conf.get("galera", "wsrep_node_address")
        auth = mysql_conf.get("galera", "wsrep_sst_auth")
        user, password = auth.split(":")
        cluster_address = mysql_conf.get("galera", "wsrep_cluster_address")
        cluster_address = cluster_address.strip('"')[len("gcomm://"):].split(",")
        if mysql_conf.has_option("galera", "wsrep_provider_options"):
            wsrep_port = mysql_conf.get("galera", "wsrep_provider_options")
        else:
            wsrep_port = ""
        wsrep_port = re.search(
            r"gmcast\.listen_addr=tcp://%s:(\d+)" % node_address,
            wsrep_port
        )
        if not wsrep_port:
            wsrep_port = 4567
        else:
            wsrep_port = int(wsrep_port.group(1))
        conn = connect_mysql(user, password, port)
    except (NoSectionError, NoOptionError):
        logger.error("not found configure for galera")
        sys.exit(signal.SIGINT)

    def exit_proc(signum=None, frame=None):
        if os.getpid() == my_pid:
            logger.warning("%s exit" % __name__)
            if conn:
                conn.close()
            sr.close()
        return

    signal.signal(signal.SIGINT, exit_proc)
    while 1:
        try:
            old_stat = sr.get_kv("galera_%s" % node_name)
            logger.info("galera_%s: " % node_name)
        except RemoteError as e:
            logger.warning(e)
            old_stat = dict()
        nodes = sr.get_kv("galera_", prefix=True)
        mariadb_conf = CONF.get(__name__, dict())
        local_role = get_local_role(nodes, cluster_address, node_address, port)
        if conn:
            status = get_wsrep_status(conn)
            variables = get_wsrep_variables(conn)
            cluster_incoming = [
                address.split(":")[0] for address in
                status["wsrep_incoming_addresses"].split(',')
            ]
            for node, info in list(nodes.items()):
                address = node.lstrip("galera_")
                if address not in cluster_incoming and info["bootstrap"]:
                    logger.warning("reset %s bootstrap False." % address)
                    info["bootstrap"] = 0
                    nodes[node] = info
                    sr.update_kv(node, info)
            wsrep_provider_options = dict(
                var.split("=") for var in
                variables["wsrep_provider_options"].replace(
                    " ", ""
                ).strip(";").split(";")
            )
            logger.debug("wsrep provider options:\n%s" %
                         pformat(wsrep_provider_options))
            if wsrep_provider_options.get("pc.bootstrap", "").lower() in (
                    "true", "yes"):
                wsrep_bootstrap = True
            else:
                wsrep_bootstrap = False
    
            if len(cluster_address) == 2:
                if local_role:
                    require_recover = False
                    if status.get("wsrep_cluster_status").lower() \
                            == "non-primary":
                        logger.warning("reset bootstrap for two-node")
                        if reset_bootstrap(conn):
                            wsrep_bootstrap = True
                else:
                    require_recover = True
                    wsrep_bootstrap = False
            else:
                require_recover = False
            wsrep_bootstrap = get_grastate(datadir) | check_bootstrap_proc() | \
                wsrep_bootstrap | local_role
    
            new_stat = {
                "name": node_name,
                "date": get_data_dir_stat(datadir),
                "bootstrap": bool(wsrep_bootstrap),
                "wsrep_ready": status.get("wsrep_ready", "OFF"),
                "wsrep_connected": status.get("wsrep_connected", "OFF"),
                "require_recover": require_recover,
            }
        else:
            wsrep_bootstrap = get_grastate(datadir) | check_bootstrap_proc() | \
                local_role
            require_recover = False
            if len(cluster_address) == 2 and not local_role:
                require_recover = True
            new_stat = {
                "name": node_name,
                "date": get_data_dir_stat(datadir),
                "bootstrap": bool(wsrep_bootstrap),
                "wsrep_ready": "OFF",
                "wsrep_connected": "OFF",
                "require_recover": require_recover,
            }
            conn = connect_mysql(user, password, port)
        if old_stat != new_stat:
            if not old_stat:
                sr.create_kv(
                    key="galera_%s" % node_address,
                    value=new_stat
                )
            else:
                logger.info("will update, old stat: %s" % old_stat)
                sr.update_kv(
                    key="galera_%s" % node_address,
                    value=new_stat
                )
            logger.info("new stat: %s" % new_stat)
        CONF["_%s_ready" % __name__] = True
        time.sleep(1)
