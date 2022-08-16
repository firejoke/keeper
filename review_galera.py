# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/7/5 17:22
import signal
import sys
from logging import getLogger

import os
import re
import time

from public_def import (
    CONF, Config, OS_SERIES, check_port, sql_execute,
    NoOptionError, NoSectionError, RemoteError
)
from srkv.api import API


logger = getLogger(__name__)


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


def check_mysql_proc(port):
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


def get_wsrep_status(user, password, port):
    try:
        status = sql_execute(user, password, port, 'show status like "wsrep%"')
        logger.debug("get wsrep status: %s" % status)
        status = {var[0]: var[1] for var in status}
        return status
    except Exception as e:
        logger.error(e)
        return None


def get_wsrep_variables(user, password, port):
    try:
        variables = sql_execute(
            user, password, port, 'show global variables like "wsrep%"'
        )
        logger.debug("get wsrep variables: %s" % variables)
        variables = {var[0]: var[1] for var in variables}
        return variables
    except Exception as e:
        logger.error(e)
        return None


def proc():
    my_pid = os.getpid()
    sr = API()

    def exit_proc(signum=None, frame=None):
        if os.getpid() == my_pid:
            logger.warning("%s exit" % __name__)
            sr.close()
        return

    signal.signal(signal.SIGINT, exit_proc)

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
        node_name = mysql_conf.get("galera", "wsrep_node_name")
    except (NoSectionError, NoOptionError):
        logger.error("not found configure for galera")
        sys.exit(signal.SIGINT)

    while 1:
        try:
            old_stat = sr.get_kv("galera_%s" % node_name)
            logger.info("galera_%s: " % node_name)
        except RemoteError as e:
            logger.warning(e)
            old_stat = dict()
        mariadb_conf = CONF.get(__name__, dict())
        m_user = mariadb_conf.get("user")
        m_password = mariadb_conf.get("pd")
        m_port = mariadb_conf.get("port")
        wsrep_status = get_wsrep_status(m_user, m_password, m_port)
        logger.debug("wsrep_status: %s" % wsrep_status)
        wsrep_variables = get_wsrep_variables(m_user, m_password, m_port)
        logger.debug("wsrep_variables: %s" % wsrep_variables)
        if wsrep_status:
            wsrep_ready = wsrep_status.get("wsrep_ready")
        else:
            wsrep_ready = "OFF"
        if wsrep_variables:
            wsrep_provider_options = {
                var.split("=")[0].strip(): var.split("=")[1].strip()
                for var in wsrep_variables[
                    "wsrep_provider_options"].strip().strip(';').split(';')
            }
            if wsrep_provider_options.get("pc.bootstrap", "").lower() in (
                    "true", "yes"):
                wsrep_bootstrap = True
            else:
                wsrep_bootstrap = False
        else:
            wsrep_bootstrap = False
        new_stat = {
            "name": node_name,
            "date": get_data_dir_stat(datadir),
            "bootstrap": get_grastate(datadir) | wsrep_bootstrap,
            "stat": check_mysql_proc(m_port),
            "wsrep_ready": wsrep_ready,
        }
        if old_stat != new_stat:
            if not old_stat:
                sr.create_kv(
                    key="galera_%s" % node_name,
                    value=new_stat
                )
            else:
                logger.info("will update, old stat: %s" % old_stat)
                sr.update_kv(
                    key="galera_%s" % node_name,
                    value=new_stat
                )
            logger.info("new stat: %s" % new_stat)

        time.sleep(1)
