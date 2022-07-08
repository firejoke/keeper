# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2019/7/15 17:19
import signal
import sys
from collections import OrderedDict
from importlib import import_module
from multiprocessing import Process

import os
import time

from public_def import (
    CONF, common_text, flush_conf, load_conf, root_path, logger,
)


if __name__ == '__main__':
    logger.info("local hostname: %s" % os.uname()[1])
    main_pid = os.getpid()
    logger.info("main pid: %s" % main_pid)
    try:
        old_conf_stat = os.stat(os.path.join(root_path, 'keeper.yaml'))
    except OSError:
        logger.error('not found config: %s' %
                     os.path.join(root_path, 'keeper.yaml'))
        sys.exit(1)
    sub_processes = OrderedDict()
    default = CONF["keeper"]
    # append srkv
    default["procs"].append({
        "name": "srkv.server",
        "reload_max": 0,
        "require": None,
    })

    for p in default["procs"]:
        # if p["name"] in sub_processes:
        #     continue
        logger.info("import package: %s" % p)
        try:
            package = import_module(p["name"])
            if float(p["reload_max"]) <= 0:
                p["reload_max"] = float("+inf")
            else:
                p["reload_max"] = float(p["reload_max"])
            p["last_reload"] = time.time()
            p["reload_number"] = 0
            sub_processes[p["name"]] = dict(
                active=True,
                target=getattr(package, "proc"),
                process=Process(target=getattr(package, "proc")),
                require=p["require"],
                reload_max=p["reload_max"]
            )
        except ImportError as e:
            logger.error(e)

    def exit_procs(signum=None, frame=None):
        if os.getpid() == main_pid:
            logger.debug(
                "Server kill.\n"
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
                sub_processes[name]["active"] = False
                if proc["process"] and proc["process"].is_alive():
                    logger.info("%s will terminate" % name)
                    os.kill(proc["process"].pid, signal.SIGINT)
            flush_conf()
            sys.exit(0)
        return

    signal.signal(signal.SIGINT, exit_procs)
    while 1:
        time.sleep(1)
        if os.path.exists(os.path.join(root_path, 'keeper.yaml')) and \
                old_conf_stat != os.stat(
                os.path.join(root_path, 'keeper.yaml')):
            logger.warning('keeper.yaml already change')
            __load_message = load_conf()
            if __load_message:
                logger.error(__load_message)
            else:
                logger.info("reload configfile")
            old_conf_stat = os.stat(os.path.join(root_path, 'keeper.yaml'))
        alive_state = list()
        for k, v in sub_processes.items():
            if not v["require"] or sub_processes[v["require"]]["process"].is_alive():
                if not v["active"]:
                    logger.info("%s.active is False" % k)
                    continue
                if v["process"].pid:
                    if not v["process"].is_alive():
                        logger.error("\"%s\" exit" % k)
                        if v["reload_number"] <= v["reload_max"]:
                            if time.time() - v["last_reload"] > 5 * 60:
                                v["reload_number"] = 0
                                v["last_reload"] = time.time()
                            v["process"].terminate()
                            logger.warning('"%s" terminate.' % k)
                            v["process"] = Process(target=v["target"])
                            logger.warning('"%s" will restart.' % k)
                            v["process"].daemon = True
                            v["process"].start()
                            v["reload_number"] += 1
                        else:
                            logger.warning(
                                '"%s": The maximum number of reloads exceeded'
                                % v["name"]
                            )
                else:
                    logger.info("\"%s\" start" % k)
                    try:
                        v["process"].daemon = True
                        v["process"].start()
                        logger.info("%s pid: %s" % (k, v["process"].pid))
                    except Exception as e:
                        logger.error(common_text(e.__repr__()))
                alive_state.append(v["process"].is_alive())
        if not any(alive_state):
            logger.error("all sub process exit")
            break
    logger.info("Service exit")
    sys.exit(1)
