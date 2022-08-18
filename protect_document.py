# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2021/7/14 14:56
"""The Module Has Been Build for..."""
import shutil
import signal
import sys
from logging import getLogger

import os
import time

from public_def import local_cmd, CONF


logger = getLogger(__name__)


def proc():
    CONF["_%s_ready" % __name__] = True
    while 1:
        backups_dir = CONF.get("conf_dirs", "./document_backups")
        if not os.path.exists(backups_dir):
            os.mkdir(backups_dir)
        backups = [
            p.replace("$", "/") for p in os.listdir(backups_dir)
        ]
        documents = CONF.get(__name__, [])
        if not isinstance(documents, list):
            logger.error("documents is invalid.")
            sys.exit(signal.SIGINT)
        for p in backups + documents:
            if os.path.exists(p):
                if p not in backups:
                    shutil.copy(
                        p,
                        os.path.join(backups_dir, p.replace("/", "$"))
                    )
                    logger.info('set backup path:%s' % p)
                    old_stat = os.stat(p)
                    logger.info('old stat: %s' % old_stat)
                    local_cmd('chattr +i %s' % p)
                    new_stat = os.stat(p)
                    logger.info('new stat: %s' % new_stat)
                if p not in documents:
                    logger.info('unset backup path: %s' % p)
                    old_stat = os.stat(p)
                    logger.info('old stat: %s' % old_stat)
                    local_cmd('chattr -i %s' % p)
                    _new_stat = os.stat(p)
                    logger.info('new stat: %s' % _new_stat)
                    os.remove(os.path.join(backups_dir, p.replace("/", "$")))
            else:
                logger.warning('%s not exists' % p)
        time.sleep(1)
