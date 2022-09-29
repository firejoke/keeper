# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/6/4 17:36
import base64
import cPickle
import inspect
import logging
import platform
from collections import OrderedDict
from copy import deepcopy
from logging.config import dictConfig
from pwd import getpwnam
from socket import AF_INET, SOCK_STREAM, socket, SHUT_RDWR
from threading import Lock, Thread

import ipaddress
import netifaces
import os
import re
import stat
import sys
from multiprocessing import Manager, get_logger

import yaml
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from invoke import Responder, UnexpectedExit, run

import MySQLdb


RedHat = ["centos", "redhat"]
Debian = ["ubuntu", "debian"]
OS_ARCH = platform.machine()
OS_NAME = platform.dist()[0]
OS_NAME = OS_NAME.lower() if OS_NAME else None
if OS_NAME in RedHat:
    OS_SERIES = "redhat"
elif OS_NAME in Debian:
    OS_SERIES = "debian"
else:
    if os.path.exists('/etc/redhat-release') or os.path.exists(
            '/lib/systemd/system/firewalld.service'):
        OS_SERIES = 'redhat'
    elif os.path.exists('/etc/debian_version') or os.path.exists(
            '/lib/systemd/system/ufw.service'):
        OS_SERIES = 'debian'
    else:
        sys.exit('OS series not found')

PYV = sys.version_info[0]
root_path = os.path.dirname(os.path.abspath(__file__))
DIR = stat.S_IFDIR
FILE = stat.S_IFREG
U_R = stat.S_IRUSR
U_W = stat.S_IWUSR
U_X = stat.S_IXUSR
U_RWX = stat.S_IRWXU
G_R = stat.S_IRGRP
G_W = stat.S_IWGRP
G_X = stat.S_IXGRP
G_RWX = stat.S_IRWXG
O_R = stat.S_IROTH
O_W = stat.S_IWOTH
O_X = stat.S_IXOTH
O_RWX = stat.S_IRWXO
A_R = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
A_X = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
A_W = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
A_RWX = A_R | A_W | A_X

ipv4_pattern = (r"("
                r"(25[0-5]|2[0-4]\d|1?\d{1,2})\."
                r"(25[0-5]|2[0-4]\d|1?\d{1,2})\."
                r"(25[0-5]|2[0-4]\d|1?\d{1,2})\."
                r"(25[0-5]|2[0-4]\d|1?\d{1,2})"
                r")")

ENV = os.environ
manager = Manager()
configuration_path = os.path.join(root_path, 'keeper.yaml')
CONF = manager.dict()
ENV.update(ETCDCTL_API="3")

__key = base64.b64encode(bytes('一把猜不到的钥匙'))
__kdf = PBKDF2HMAC(
    algorithm=hashes.SHA512(),
    length=32,
    salt=__key[:16],
    iterations=1000000,
    backend=default_backend()
)
fernet = Fernet(
    base64.urlsafe_b64encode(
        __kdf.derive(
            base64.b64encode(bytes(__key[:32]))
        )
    )
)

__sr_default_port = 666
__sr_default_heartbeat = 150 * (10 ** -3)
__sr_base_timeout = 1

db_url = "sqlite:///%s" % os.path.join(root_path, ".keeper.db")
SRkvLocalSock = os.path.join(root_path, "srkv.sock")
SRkvNodeRole = {
    0: "Leader",
    1: "Candidate",
    2: "Follower",
}


if PYV == 3:
    from configparser import ConfigParser, NoOptionError, NoSectionError

    class Config(ConfigParser):

        def optionxform(self, optionstr):
            return optionstr

        def set(self, section, option, value=None):
            logger.info("set %s=%s for %s" % (option, value, section))
            super(Config, self).set(section, option, value)

        def remove_option(self, section, option):
            logger.warning("remove %s for %s" % (option, section))
            super(Config, self).remove_option(section, option)

        def remove_section(self, section):
            logger.warning("remove section: %s" % section)
            super(Config, self).remove_section(section)

        def add_section(self, section):
            logger.info('add section: %s' % section)
            super(Config, self).add_section(section)

    encoding_type = str
elif PYV == 2:
    from ConfigParser import ConfigParser, NoOptionError, NoSectionError

    class Config(ConfigParser):

        def optionxform(self, optionstr):
            return optionstr

        def set(self, section, option, value=None):
            logger.info("set %s=%s for %s" % (option, value, section))
            ConfigParser.ConfigParser.set(self, section, option, value)

        def remove_option(self, section, option):
            logger.warning("remove %s for %s" % (option, section))
            ConfigParser.ConfigParser.remove_option(self, section, option)

        def remove_section(self, section):
            logger.warning("remove section: %s" % section)
            ConfigParser.ConfigParser.remove_section(self, section)

        def add_section(self, section):
            logger.info('add section: %s' % section)
            ConfigParser.ConfigParser.add_section(self, section)

    encoding_type = unicode


class ColorFormatter(logging.Formatter):
    @staticmethod
    def colorize(text='', opts=(), **kwargs):
        """
        from Django
        ========================================================================

        Return your text, enclosed in ANSI graphics codes.
        Depends on the keyword arguments 'fg' and 'bg', and the contents of
        the opts tuple/list.
        Return the RESET code if no parameters are given.
        Valid colors:
            'black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white'
        Valid options:
            'bold'
            'underscore'
            'blink'
            'reverse'
            'conceal'
            'noreset' - string will not be auto-terminated with the RESET code
        Examples:
            colorize('hello', fg='red', bg='blue', opts=('blink',))
            colorize()
            colorize('goodbye', opts=('underscore',))
            print(colorize('first line', fg='red', opts=('noreset',)))
            print('this should be red too')
            print(colorize('and so should this'))
            print('this should not be red')
        """
        color_names = (
            'black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan',
            'white'
        )
        foreground = {color_names[x]: '3%s' % x for x in range(8)}
        background = {color_names[x]: '4%s' % x for x in range(8)}

        reset = '0'
        opt_dict = {
            'bold': '1', 'underscore': '4', 'blink': '5', 'reverse': '7',
            'conceal': '8'
        }
        code_list = []
        if text == '' and len(opts) == 1 and opts[0] == 'reset':
            return '\x1b[%sm' % reset
        for k, v in kwargs.items():
            if k == 'fg':
                code_list.append(foreground[v])
            elif k == 'bg':
                code_list.append(background[v])
        for o in opts:
            if o in opt_dict:
                code_list.append(opt_dict[o])
        if 'noreset' not in opts:
            text = '%s\x1b[%sm' % (text or '', reset)
        return '%s%s' % (('\x1b[%sm' % ';'.join(code_list)), text or '')

    @classmethod
    def set_color(cls, levelname, msg):
        if levelname in ("CRITICAL", "FATAL"):
            msg = cls.colorize(msg, fg="red", opts=("bold", "reverse"))
        elif levelname == "ERROR":
            msg = cls.colorize(msg, fg="red", opts=("bold",))
        elif levelname in ("WARN", "WARNING"):
            msg = cls.colorize(msg, fg="yellow", opts=("bold",))
        elif levelname == "INFO":
            msg = cls.colorize(msg, fg="green")
        elif levelname == "DEBUG":
            msg = cls.colorize(msg, fg="magenta", opts=("bold",))
        return msg

    def format(self, record):
        record.msg = self.set_color(record.levelname, record.msg)
        return super(ColorFormatter, self).format(record)

    def formatException(self, ei):
        msg = super(ColorFormatter, self).formatException(ei)
        return self.colorize(msg, fg="red", opts=("bold", "reverse"))


def ip_check(ip_address):
    ip_address = encoding_type(ip_address)

    try:
        ipaddress.ip_address(ip_address)
        return True
    except (ValueError, ipaddress.AddressValueError):
        return False


def load_conf():
    global CONF
    try:
        message = ""
        with open(configuration_path, 'r') as f:
            conf = yaml.safe_load(f)
        if not conf:
            conf = dict()
        default = conf.get("keeper", dict())
        srkv = conf.get("srkv", dict())
        alter_key = default.get('alter_key', None)
        if alter_key:
            try:
                alter_key = fernet.decrypt(bytes(alter_key))
            except InvalidToken:
                pass
        if CONF.items() and alter_key != 'keeper':
            return 'alter key error'
        CONF.update(conf)
        log = default.get("logging", dict())
        if not log:
            log = {
                "level": "INFO",
                "dir": "/var/log/keeper/",
                "when": "W0",
                "backupCount": 6,
            }
            default["logging"] = log
        elif not isinstance(log, dict):
            raise RuntimeError("Logging configuration type error, must be dict")
        elif set(log) != {"level", "dir", "when", "backupCount"}:
            raise RuntimeError("Logging configuration keys error.")

        if not os.path.exists(log["dir"]):
            os.mkdir(log["dir"])
        if not os.path.exists(os.path.join(log["dir"], "procs")):
            os.mkdir(os.path.join(log["dir"], "procs"))
        if log["level"] and log["level"].lower() not in (
                "info", "warn", "debug", "error"):
            message = 'LOG_Level value is invalid, will be set "WARN"'
            log["level"] = "WARN"
        elif not log["level"]:
            log["level"] = "WARN"
        log["level"] = log["level"].upper()
        default["logging"] = log

        for proc in default.get("procs", list()):
            if not isinstance(proc, dict) or \
                    not set(proc.keys()) <= {"name", "reload_max", "requires"}:
                raise RuntimeError("supervisory.procs configuration error")
            if not isinstance(proc["reload_max"], (int, float)):
                raise RuntimeError("The value of reload_max has to be a number")
        CONF["keeper"] = default
        # srkv config
        if not set(srkv.keys()) <= {
            "heartbeat", "port", "timeout", "scan", "nodes", "exclude_ipaddress"
        }:
            raise RuntimeError("srkv configuration error")
        if "port" not in srkv:
            srkv["port"] = __sr_default_port
        elif not isinstance(srkv["port"], int):
            raise TypeError("srkv.port must be an integer")
        if "heartbeat" not in srkv:
            srkv["heartbeat"] = __sr_default_heartbeat
        elif not isinstance(srkv["heartbeat"], float):
            raise TypeError("srkv.heartbeat must be a float")
        if "timeout" not in srkv:
            srkv["timeout"] = __sr_base_timeout
        elif not isinstance(srkv["timeout"], (int, float)):
            raise TypeError("srkv.timeout must be a number")
        if "scan" not in srkv:
            srkv["scan"] = True
        elif not isinstance(srkv["scan"], bool):
            raise TypeError("srkv.scan must be an boolean type")
        if "nodes" not in srkv:
            srkv["nodes"] = []
        elif not isinstance(srkv["nodes"], list):
            raise TypeError("srkv.nodes must be a list.")
        for n in srkv["nodes"]:
            if not ip_check(n):
                raise TypeError(
                    "srkv.nodes must be a valid ipaddress list"
                )
        CONF["srkv"] = srkv

        return message
    except IOError:
        raise IOError('not found config: %s' % configuration_path)
    except yaml.YAMLError as e:
        raise SyntaxError("yaml syntax error: " + e.__str__())


__load_message = load_conf()
__log_level = CONF["keeper"]["logging"]["level"]
__log_dir = CONF["keeper"]["logging"]["dir"]
__procs_log_dir = os.path.join(__log_dir, "procs")
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "debug": {
            "()": ColorFormatter,
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "[%(processName)s(%(process)d):"
                      "%(threadName)s(%(thread)d)] "
                      "%(pathname)s[%(funcName)s:%(lineno)d] - "
                      "%(message)s"
        },
        "verbose": {
            "()": ColorFormatter,
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "%(module)s [%(funcName)s:%(lineno)d] "
                      "- %(message)s"
        },
        "simple": {
            "()": ColorFormatter,
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "%(module)s [%(funcName)s] "
                      "- %(message)s"
        },
    },
    "handlers": {
        "root": {
            "level": __log_level,
            "filename": os.path.join(__log_dir, "keeper.log"),
        },
        "console": {
            "level": __log_level,
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "sql": {
            "level": __log_level,
            "filename": os.path.join(__log_dir, "sql.log"),
        },
        "zerorpc": {
            "mixin": False,
            "level": __log_level,
            "filename": os.path.join(__log_dir, "zerorpc.log"),
            "class": "logging.handlers.RotatingFileHandler",
            "maxBytes": 100 * 1024 * 1024,
            "backupCount": 6,
            "formatter": "debug" if __log_level == "DEBUG" else "verbose",
        },
        "srkv": {
            "level": __log_level,
            "filename": os.path.join(__log_dir, "srkv.log"),
        },
    },
    "loggers": {
        "root": {
            "handlers": ["root"],
            "level": __log_level
        },
        "multiprocessing": {
            "handlers": ["root"],
            "level": __log_level
        },
        "sqlalchemy": {
            "handlers": ["sql"],
            "level": __log_level
        },
        "alembic": {
            "handlers": ["console", "sql"],
            "level": __log_level
        },
        "zerorpc": {
            "handlers": ["zerorpc"],
            "level": __log_level
        },
        "srkv": {
            "handlers": ["srkv"],
            "level": __log_level
        },
    },
}

for proc in CONF["supervisory"].get("procs"):
    LOGGING["handlers"][proc["name"]] = {
        "level": __log_level,
        "filename": os.path.join(__procs_log_dir, "%s.log" % proc["name"])
    }
    LOGGING["loggers"][proc["name"]] = {
        "handlers": [proc["name"]],
        "level": __log_level
    }
__file_handler_mixin = {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": CONF["keeper"]["logging"]["when"],
            "backupCount": CONF["keeper"]["logging"]["backupCount"],
            "formatter": "debug" if __log_level == "DEBUG" else "verbose",
        }
for name, handler in LOGGING["handlers"].items():
    if "filename" in handler:
        if handler.get("mixin", True):
            handler.update(__file_handler_mixin)
        handler.pop("mixin", None)

dictConfig(LOGGING)

logger = get_logger()
logger.debug("Proxy object pid: %s" % manager._process.pid)
if __load_message:
    logger.error(__load_message)


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from zerorpc import Client, RemoteError


def flush_conf():
    global CONF
    _d = deepcopy(CONF)
    _d["keeper"]['alter_key'] = None
    for k in _d.keys():
        if k.startswith("_") and k.endswith("ready"):
            logger.debug("conf pop %s" % k)
            _d.pop(k)
    with open(configuration_path, 'w') as f:
        _conf = yaml.safe_dump(_d, default_flow_style=False)
        logger.info("flush configuration:\n%s" % _conf)
        f.write(_conf)


def get_base_methods(cls, obj):
    cls_methods = [
        m[0] for m in inspect.getmembers(cls)
        if callable(m[1]) and not m[0].startswith("_")
    ]
    return [
        m[0] for m in inspect.getmembers(obj)
        if m[0] not in cls_methods and callable(m[1])
    ]


def encrypt_obj(obj):
    obj = cPickle.dumps(obj)
    return fernet.encrypt(bytes(obj))


def decrypt_text(text):
    text = fernet.decrypt(bytes(text))
    return cPickle.loads(text)


def common_text(msg):
    if isinstance(msg, unicode):
        msg = msg.encode('utf-8')
    return msg


def local_cmd(command, interactions=None, **kwargs):
    """
    r = r"(?i)y\\|n|\\[y/d/n\\]\\|\\[y/n\\]\\|y/n"
    watcher = Responder(pattern=r, response='y\n')
    :param command: linux 命令
    :param interactions: [交互命令的匹配规则, ]
    :return: (stdout, stderr)
    """
    if interactions and isinstance(interactions, (list, tuple)):
        watcher = [
            Responder(pattern=interaction['pattern'],
                      response=interaction['response'])
            for interaction in interactions
        ]
    else:
        watcher = None
    try:
        res = run(command, watchers=watcher, warn=True, hide=True, **kwargs)
        if isinstance(res.stdout, str):
            _stdout = res.stdout.decode('utf-8')
        else:
            _stdout = res.stdout
        if isinstance(res.stderr, str):
            _stderr = res.stderr.decode('utf-8')
        else:
            _stderr = res.stderr
        res = _stdout, _stderr
    except UnexpectedExit as e:
        res = ('', e)
    return res


def chown(path, owner, group, recursion=False):
    if not isinstance(owner, int):
        owner = getpwnam(owner).pw_uid
    if not isinstance(group, int):
        group = getpwnam(group).pw_gid
    if path.endswith("*") and not os.path.exists(path):
        prefix = os.path.basename(path[:-1])
        path = os.path.dirname(path)
    else:
        prefix = None
    if prefix is None:
        try:
            os.chown(common_text(path), owner, group)
        except OSError as e:
            if not re.search(r'(?i)\bNo such\b|没有', common_text(e.__str__())):
                raise e
        finally:
            return
    if os.path.isdir(path) and (prefix is not None or recursion):
        for p in os.listdir(path):
            if prefix is not None and not p.startswith(prefix):
                continue
            chown(path, owner, group, recursion)


def chmod(path, permission, join=True, recursion=False):
    if join:
        permission = permission | os.stat(path).st_mode
    if path.endswith("*") and not os.path.exists(path):
        prefix = os.path.basename(path[:-1])
        path = os.path.dirname(path)
    else:
        prefix = None
    if prefix is None:
        try:
            os.chmod(common_text(path), permission)
        except OSError as e:
            if not re.search(r'(?i)\bNo such\b|没有', common_text(e.__str__())):
                raise e
        finally:
            return

    if os.path.isdir(path) and (prefix is not None or recursion):
        for p in os.listdir(path):
            if prefix is not None and not p.startswith(prefix):
                continue
            chmod(
                os.path.join(path, p), permission,
                join=join, recursion=recursion
            )


def read_hosts():
    hs = OrderedDict()
    if os.path.exists("/etc/hosts"):
        with open("/etc/hosts") as f:
            for h in f.readlines():
                # _res = re.search(
                #     r"%s\s+([a-zA-Z][a-zA-Z0-9.\-]*[a-zA-Z0-9])\s*(.*)" %
                #     ipv4_pattern, h
                # )
                _res = h.strip().split()
                if _res and len(_res) > 1:
                    hs[_res[0]] = [_res[1], " ".join(_res)]
        return hs, os.path.getmtime("/etc/hosts")
    return hs, 0


def get_host_ip(hostname, hosts):
    host_ip = ""
    for _ip, hs in hosts.items():
        if hs[0] == hostname:
            host_ip = _ip
    return host_ip


def get_local_interfaces():
    interfaces = []
    for _if in netifaces.interfaces():
        if not _if.startswith("virbr"):
            for _ip in netifaces.ifaddresses(_if).get(netifaces.AF_INET,
                                                      tuple()):
                if _ip.get("addr") not in (None, "127.0.0.1"):
                    interfaces.append(_ip)
    return interfaces


def check_port(port, ipaddres="127.0.0.1", timeout=0.1):
    s = socket(AF_INET, SOCK_STREAM)
    s.settimeout(timeout)
    if not ip_check(ipaddres):
        return False, "ipaddres not valid"
    try:
        res = s.connect_ex((ipaddres, port))
        if res:
            return False, res
        s.shutdown(SHUT_RDWR)
        return True, res
    except Exception as e:
        return False, "Failed to connect to %s:%s : %s" % (ipaddres, port, e)
    finally:
        s.close()


def scan_port(network_segment, netmask, port):
    # prefix_len = netmask.rstrip(".0").split(".").__len__()
    # prefix = ".".join(network_segment.split(".")[:prefix_len])
    # _ips = [prefix]
    # for i in range(4-prefix_len):
    #     pl = []
    #     for p in _ips:
    #         for e in range(256):
    #             pl.append("%s.%s" % (p, e))
    #     _ips = pl
    logger.debug(
        "network segment: %s, netmask: %s, port: %s" %
        (network_segment, netmask, port)
    )
    ips = []
    lock = Lock()

    def connect(ip):
        try:
            if check_port(port, ip)[0]:
                lock.acquire(True)
                ips.append(ip)
                lock.release()
            return
        except Exception as e:
            logger.error(e)

    i = 1
    t = None
    for _ip in ipaddress.ip_network(
            "%s/%s" % (network_segment, netmask), strict=False
    ).hosts():
        if i == 65535:
            i = 1
        logger.debug("try connect %s" % _ip)
        t = Thread(target=connect, args=(str(_ip),))
        t.start()
        i += 1
    else:
        if t:
            t.join()
    return ips


def sql_execute(user, password, port, sql):
    db = MySQLdb.connect(
        host="localhost", user=user, passwd=password, port=port
    )
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        return list(cursor)
    # except MySQLdb.ProgrammingError as e:
    #     msg = common_text(e.__str__())
    #     return None, msg
    finally:
        db.close()

class RpcClient(Client):
    def __init__(self, *args, **kwargs):
        Client.__init__(self, *args, **kwargs)
        self._base_method = get_base_methods(RpcClient, self)

    def __call__(self, method, *args, **kwargs):
        if not method.startswith("_") and method not in self._base_method:
            encrypt_args = encrypt_obj(
                {
                    "args": args,
                    "kwargs": kwargs
                }
            )
            return decrypt_text(Client.__call__(self, method, encrypt_args))
        return Client.__call__(self, method, *args, **kwargs)

db_engine = create_engine(db_url)
ModelBase = declarative_base()
Session = sessionmaker(bind=db_engine)
