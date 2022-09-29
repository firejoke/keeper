# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2019/7/15 17:19
import signal
import sys
from collections import OrderedDict
from functools import wraps
from importlib import import_module
from multiprocessing import Process
from traceback import format_exception

import os
import time

from public_def import configuration_path, CONF, flush_conf, load_conf, logger


def run_module(func):
    @wraps(func)
    def wrapped():
        try:
            return func()
        except Exception:
            traceback_exc = "".join(format_exception(*sys.exc_info()))
            logger.error(
                "%s exception:\n%s" % (func.__module__, traceback_exc)
            )
    return wrapped


if __name__ == '__main__':
    logger.info("local hostname: %s" % os.uname()[1])
    main_pid = os.getpid()
    logger.info("main pid: %s" % main_pid)
    try:
        old_conf_stat = os.stat(configuration_path)
    except OSError:
        logger.error('not found config: %s' % configuration_path)
        sys.exit(1)
    sub_processes = OrderedDict()
    default = CONF["keeper"]
    default["procs"].insert(
        0,
        {
            "name": "srkv.server",
            "reload_max": 0,
            "requires": None,
        }
    )

    for p in default["procs"]:
        if p["name"].startswith("_"):
            logger.error("The process name cannot begin with a _")
            continue
        logger.info("import package: %s" % p)
        try:
            package = import_module(p["name"])
            if float(p["reload_max"]) <= 0:
                p["reload_max"] = float("+inf")
            else:
                p["reload_max"] = float(p["reload_max"])
            if not isinstance(p.get("requires"), list):
                if p.get("requires"):
                    logger.warning("require must list")
                p["requires"] = list()
            if p["name"] != "srkv.server":
                p["requires"].insert(0, "srkv.server")
            _proc = run_module(getattr(package, "proc"))
            sub_processes[p["name"]] = dict(
                module=package,
                active=True,
                target=_proc,
                process=Process(target=_proc),
                requires=p["requires"],
                reload_max=p["reload_max"],
                last_reload=time.time(),
                reload_number=0
            )
            CONF["_%s_ready" % p["name"]] = False
        except Exception:
            logger.error("\n%s" % "".join(format_exception(*sys.exc_info())))


    def exit_procs(signum=None, frame=None):
        if os.getpid() == main_pid:
            logger.info("Server kill.\n")
            logger.debug(
                "signum: {0}\nframe:\n\tf_back:{1}\n\tf_builtins:{2}\n"
                "\tframe.f_code:{3}\n\tframe.f_exc_traceback:{4}\n"
                "\tframe.f_exc_type:{5}\n\tframe.f_exc_value:{6}\n"
                "\tframe.f_globals:{7}\n\tframe.f_lasti:{8}\n"
                "\tframe.f_lineno:{9}\n\tframe.f_locals:{10}\n"
                "\tframe.f_restricted:{11}\n\tframe.f_trace:{12}\n".format(
                    signum, frame.f_back, frame.f_builtins, frame.f_code,
                    frame.f_exc_traceback, frame.f_exc_type, frame.f_exc_value,
                    frame.f_globals, frame.f_lasti, frame.f_lineno,
                    frame.f_locals, frame.f_restricted, frame.f_trace
                )
            )
            for name, proc in sub_processes.items():
                if name == "srkv.server":
                    continue
                proc["active"] = False
                if proc["process"] and proc["process"].is_alive():
                    logger.info("%s will terminate" % name)
                    os.kill(proc["process"].pid, signal.SIGINT)
                CONF["_%s_ready" % name] = False
            os.kill(sub_processes["srkv.server"]["process"].pid, signal.SIGINT)
            CONF["_srkv.server_ready"] = False
            try:
                flush_conf()
            except Exception:
                traceback_exc = "".join(format_exception(*sys.exc_info()))
                logger.error(traceback_exc)
            sys.exit(0)
        return

    signal.signal(signal.SIGINT, exit_procs)
    while 1:
        time.sleep(1)
        if os.path.exists(configuration_path) \
                and old_conf_stat != os.stat(configuration_path):
            logger.warning('keeper.yaml already change')
            __load_message = load_conf()
            if __load_message:
                logger.error(__load_message)
            else:
                logger.info("reload configfile")
            old_conf_stat = os.stat(configuration_path)
        alive_state = list()
        for k, v in sub_processes.items():
            for _m in v["requires"]:
                if not sub_processes.get(_m):
                    logger.error("%s not import, %s will be remove" % (_m, k))
                    sub_processes.pop(k)
                    break
                if not sub_processes[_m]["process"].is_alive():
                    logger.warning(
                        "%s is not alive, %s will not load" % (_m, k)
                    )
                    break
                if not CONF["_%s_ready" % _m]:
                    logger.warning(
                        "%s is not ready, %s will not load." % (_m, k)
                    )
                    break
            else:
                if not v["active"]:
                    logger.info("%s.active is False" % k)
                    continue
                if v["process"].pid:
                    if not v["process"].is_alive():
                        logger.error("\"%s\" exit" % k)
                        v["process"].terminate()
                        logger.warning('"%s" terminate.' % k)
                        reload(v["module"])
                        v["target"] = run_module(getattr(v["module"], "proc"))
                        v["process"] = Process(target=v["target"])
                        logger.warning('"%s" will restart.' % k)
                else:
                    if v["reload_number"] <= v["reload_max"]:
                        if time.time() - v["last_reload"] > 5 * 60:
                            v["reload_number"] = 0
                            v["last_reload"] = time.time()
                        logger.info("\"%s\" start" % k)
                        try:
                            v["process"].daemon = True
                            v["process"].start()
                            v["reload_number"] += 1
                            logger.info("%s pid: %s" % (k, v["process"].pid))
                        except Exception as e:
                            logger.error(e)
                    else:
                        logger.warning(
                            '"%s": The maximum number of reloads exceeded' % k
                        )
                        sub_processes.pop(k)
                alive_state.append(v["process"].is_alive())
        if not any(alive_state):
            logger.error("all sub process exit")
            break
    logger.info("Service exit")
    sys.exit(signal.SIGINT)
