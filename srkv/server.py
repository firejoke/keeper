# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2022/5/19 17:23
import json
from traceback import format_exception

import signal
from collections import Iterator, Mapping, OrderedDict, Sequence, Set
from functools import wraps
from logging import getLogger

import os
from copy import deepcopy, copy

from gevent import sleep, spawn
from gevent.event import Event as gEvent
from random import uniform

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import lazyload
from time import time
from zerorpc import (
    LostRemote, RemoteError, Server, TimeoutExpired, stream as z_stream
)

from models import Nodes, Repository, Transaction
from public_def import (
    CONF, RpcClient, SRkvLocalSock, SRkvNodeRole, Session,
    decrypt_text, encrypt_obj, get_local_interfaces, ip_check, scan_port,
)


logger = getLogger(__package__)


def check_params(func):
    @wraps(func)
    def wrapped(self, en_text):
        params = decrypt_text(en_text)
        if not isinstance(params, dict):
            raise TypeError("The parameter type must be a dict")
        args = params.get("args", tuple())
        kwargs = params.get("kwargs", dict())
        return func(self, *args, **kwargs)

    return wrapped


def encrypt_result(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        result = encrypt_obj(func(self, *args, **kwargs))
        return result
    return wrapped


def encrypt_stream(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if isinstance(result, (Iterator, Sequence, Set)):
            return (encrypt_obj(e) for e in result)
        elif isinstance(result, Mapping):
            return (encrypt_obj((k, v)) for k, v in result.items())
        else:
            raise TypeError("result type is not Iterable")
    return wrapped


class Node(Server):
    """
     Node of Raft cluster
      属性：
        当前节点的唯一属性 sys_uid （不可变属性）
        当前节点的网络接口 interfaces （在展示自身信息的时候更新）
        自身的领袖是谁 leader
        自身的角色 role
        自身的任期 term_count
        是否已经投票 have_voted
        当自身为leader时的心跳时间 heartbeat
        自身的超时时间 timeout
        集群共有存储信息 store
        集群节点信息 cluster_nodes

      功能：
        展示自身的属性
        给别人投票
        请求别人投票给自己

   """
    ipaddresses = {_ip["addr"] for _ip in get_local_interfaces()}

    with open("/sys/class/dmi/id/product_uuid") as f:
        sys_uid = f.read().strip()

    def __init__(self, config):
        self._config = config
        self.ipaddresses = list(
            self.ipaddresses - set(self._config.get("exclude_ipaddress", []))
        )
        self._leader = None
        self.leader_uid = None
        self.leader_keepalive = False
        self.role = 1
        self.term_counter = 0
        self.have_voted = False
        self.heartbeat = self._config.get("heartbeat")
        self.base_timeout = self._config.get("timeout")
        self.timeout = self.base_timeout + self.heartbeat + uniform(0.5, 2.5)
        self.rpc_port = self._config["port"]
        self.votes = dict()
        self.remote_nodes = dict()
        self.join_nodes = list()
        self._srcr_status = {"me": self.sys_uid, "ready": dict()}
        self._db_session = Session()
        self.repository = dict()
        self._repository_ready = False
        self.repository_transaction = OrderedDict()
        # 待执行的事务编号
        self.repository_transaction_id = 0
        self._repository_transaction_ready = False
        self._repository_transaction_event = gEvent()
        self._repository_transaction_daemon = None
        self._repository_transaction_daemon_exit = 0
        self._cluster_daemon = None
        self._cluster_daemon_exit = 0
        Server.__init__(self, heartbeat=self.heartbeat)
        try:

            if not self._db_session.query(Nodes).get(self.sys_uid):
                _local = Nodes(
                    sys_uid=self.sys_uid,
                    ipaddresses=json.dumps(self.ipaddresses)
                )
                self._db_session.add(_local)
                self._db_session.commit()
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error(e)

    def _check_transaction_ready(self):
        now = time()
        while not self._repository_transaction_ready:
            if time() - now <= self.base_timeout:
                sleep(max(1, self.heartbeat / 2.0))
            else:
                raise RuntimeError("transaction is not ready.")
        else:
            return True

    def _check_repository_ready(self):
        now = time()
        while not self._repository_ready:
            if time() - now <= self.base_timeout:
                sleep(max(1, self.heartbeat / 2.0))
            else:
                raise RuntimeError("repository is not ready.")

    def _check_srcr(self, ):
        if not all(self._srcr_status["ready"].values()):
            raise RuntimeError("srkv cluster is not ready.")

    @check_params
    @encrypt_result
    def info(self):
        self._check_transaction_ready()
        return {
            "sys_uid": self.sys_uid,
            "leader": self.leader_uid,
            "role": self.role,
            "ipaddresses": self.ipaddresses,
            "term_counter": self.term_counter,
            "have_voted": self.have_voted,
            "votes": self.votes,
            "remote_nodes": self.remote_nodes.keys(),
            "transaction_id": self.repository_transaction_id,
            "transaction_ready": self._repository_transaction_ready
        }

    @check_params
    @encrypt_result
    def keepalive(self, leader_uid, leader_transaction_id):
        if leader_uid == self.leader_uid:
            self.leader_keepalive = True
            return leader_uid
        elif not self.role == 1:
            return None
        elif (
                self.role == 2 and
                leader_transaction_id > self._leader.info()["transaction_id"]
        ) or (
                self.role == 0 and
                leader_transaction_id > self.repository_transaction_id
        ):
            self.leader_keepalive = False
            self._leader = None
            self.role = 1
            self.have_voted = False
            self._repository_ready = False
            return leader_uid
        else:
            return self.leader_uid

    def _send_keepalive(self, uid):
        # logger.debug("send keepalive to %s" % uid)
        try:
            if not self.remote_nodes[uid]:
                logger.error("node(%s) is offline." % uid)
                logger.info("try connect node(%s)" % uid)
                node = self._db_session.query(Nodes).get(uid)
                c, info = self._connect_node(json.loads(node.ipaddresses))
                if c:
                    self.remote_nodes[uid] = c
                    self._srcr_status["ready"][uid] = False
                else:
                    logger.error("connection to node(%s) failed" % uid)
            else:
                node_leader = self.remote_nodes[uid].keepalive(
                    self.sys_uid, self.repository_transaction_id
                )
                if node_leader and node_leader != self.sys_uid:
                    logger.warning(
                        "The lead node of the remote node is not the local node"
                        ", the election is restarted"
                    )
                    self.role = 1
                    self._leader = None
                    self.leader_uid = None
                    self.have_voted = False
                    self._repository_ready = False
                else:
                    self._srcr_status["ready"][uid] = self.remote_nodes[
                        uid].info()["transaction_ready"]
        except (LostRemote, TimeoutExpired) as e:
            self.remote_nodes[uid].close()
            self.remote_nodes[uid] = None
            if uid in self._srcr_status:
                self._srcr_status["ready"].pop(uid)
            logger.error("connect node(%s) failed: \n%s" % (uid, e))
        except RemoteError as e:
            logger.error("node(%s) remote error: \n%s" % (uid, e))
        return

    @check_params
    @encrypt_result
    def vote(self, node_uid, node_term_counter, transaction_id):
        if self.role == 2:
            _transaction_max_id = self._leader.info()["transaction_id"]
        else:
            _transaction_max_id = self.repository_transaction_id
        if transaction_id > _transaction_max_id or (
                transaction_id == _transaction_max_id and
                node_term_counter >= self.term_counter
                and not self.have_voted
        ):
            self._check_transaction_ready()
            self.have_voted = True
            self.term_counter += 1
            self._leader = self.remote_nodes[node_uid]
            self.leader_uid = node_uid
            logger.info("my leader: %s" % self.leader_uid)
            self.role = 2
            self.votes[node_uid] = 1
            self._repository_ready = False
            self._repository_transaction_ready = False
            return node_uid
        return self.leader_uid

    def _seek_votes(self):
        logger.info("seek votes")
        self.term_counter += 1
        logger.info("my term counter: %s" % self.term_counter)
        self.role = 1
        self.have_voted = True
        self.votes[self.sys_uid] = 1
        for _uid in copy(self.remote_nodes.keys()):
            logger.info("canvass votes for node(%s)" % _uid)
            try:
                node_leader = self.remote_nodes[_uid].vote(
                    self.sys_uid, self.term_counter,
                    self.repository_transaction_id
                )
                logger.info("node(%s) of leader: %s" % (_uid, node_leader))
                if node_leader in self.votes:
                    self.votes[node_leader] += 1
                elif node_leader:
                    self.votes[node_leader] = 1
            except (LostRemote, TimeoutExpired) as e:
                self.remote_nodes[_uid].close()
                self.remote_nodes.pop(_uid)
                if _uid in self._srcr_status:
                    self._srcr_status["ready"].pop(_uid)
                logger.error(
                    "connect failed, "
                    "node(%s) will be removed from remote_nodes: \n%s" %
                    (_uid, e)
                )
            except RemoteError as e:
                logger.error("failed to canvass for node(%s): \n%s" % (_uid, e))
                return False
        return True

    def _election(self):
        logger.info("My uid: %s" % self.sys_uid)
        if self.role == 1:
            logger.info("start election")
            self._join_remote_nodes()
            logger.info("remote nodes: %s" % self.remote_nodes)
            win_votes = (len(self.remote_nodes) + 1) / 2.0
            self.votes.clear()
            if not self._seek_votes():
                return False
            win_nodes = [
                (uid, votes) for uid, votes in self.votes.items()
                if votes >= win_votes
            ]
            if len(win_nodes) == 2 and (self.sys_uid, 1) in win_nodes:
                win_nodes.remove((self.sys_uid, 1))
            logger.info("win nodes: %s" % win_nodes)
            if len(win_nodes) > 1:
                logger.error("Many leaders: %s" % win_nodes)
                return False
            elif len(win_nodes) == 1:
                self.leader_uid = win_nodes[0][0]
            elif not win_nodes:
                if not self.remote_nodes:
                    self.leader_uid = self.sys_uid
                else:
                    self.have_voted = False
                    logger.error("No leader was elected")
                    return False
            if self.leader_uid == self.sys_uid:
                self.role = 0
                self._leader = self
                self._repository_ready = True
                self._repository_transaction_ready = True
                CONF["_%s_ready" % __name__] = True
            else:
                self.role = 2
                self._leader = self.remote_nodes[self.leader_uid]
                self.leader_keepalive = True
                self._repository_ready = False
                self._repository_transaction_ready = False

        return True

    @check_params
    @encrypt_result
    def echo(self, node_uid, node_ipaddress):
        if not self.remote_nodes.get(node_uid, None):
            self.join_nodes.append(
                {"sys_uid": node_uid, "ipaddresses": node_ipaddress}
            )

        return {"sys_uid": self.sys_uid, "ipaddresses": self.ipaddresses}

    def _check_join_node(self):
        logger.info("Check the added nodes.")
        for _node in deepcopy(self.join_nodes):
            self.join_nodes.remove(_node)
            if not self.remote_nodes.get(_node["sys_uid"], None):
                logger.info("try connect node(%s)" % _node)
                c = self._connect_node(_node["ipaddresses"])[0]
                if c:
                    self.remote_nodes[_node["sys_uid"]] = c
                    self._srcr_status["ready"][_node["sys_uid"]] = False
                else:
                    self.join_nodes.append(_node)
                    return
            if not self._db_session.query(Nodes).get(_node["sys_uid"]):
                try:
                    logger.info("save node(%s) to database: %s" %
                                (_node["sys_uid"], _node))
                    node = Nodes(
                        sys_uid=_node["sys_uid"],
                        ipaddresses=json.dumps(_node["ipaddresses"])
                    )
                    self._db_session.add(node)
                    self._db_session.commit()
                except SQLAlchemyError as e:
                    self.join_nodes.append(_node)
                    logger.error(
                        "node(%s) save failed: \n%s" % (_node["sys_uid"], e)
                    )
                finally:
                    self._db_session.rollback()

    def _connect_node(self, ipaddresses):
        c = RpcClient(heartbeat=self.heartbeat)
        for ip in ipaddresses:
            _url = "tcp://%s:%s" % (ip, self.rpc_port)
            try:
                logger.debug("connection to node(%s)" % _url)
                c.connect(_url)
                info = c.echo(self.sys_uid, self.ipaddresses)
            except (LostRemote, TimeoutExpired, RemoteError) as e:
                c.disconnect(_url)
                logger.error("connect node(%s) failed: %s" % (ip, e))
                continue
            else:
                return c, info
        c.close()
        return None, None

    def _join_remote_nodes(self, ):
        for node in self._db_session.query(Nodes).filter(
                Nodes.sys_uid != self.sys_uid
        ).all():
            if self.remote_nodes.get(node.sys_uid, None):
                continue
            c = self._connect_node(json.loads(node.ipaddresses))[0]
            if c:
                self.remote_nodes[node.sys_uid] = c
                self._srcr_status["ready"][node.sys_uid] = False
        return self.remote_nodes

    def _save_nodes(self, ipaddresses):
        exists_nodes = self._db_session.query(
            Nodes.sys_uid, Nodes.ipaddresses).all()
        exists_uids = []
        exists_ips = []
        logger.info("exists nodes: %s" % exists_nodes)
        for n in exists_nodes:
            exists_uids.append(n[0])
            exists_ips += json.loads(n[1])
        for ip in ipaddresses:
            if ip_check(ip) and ip not in exists_ips:
                c, info = self._connect_node([ip])
                if not c:
                    continue
                try:
                    if info["sys_uid"] in exists_uids:
                        logger.info(
                            "update node(%s): %s" % (info["sys_uid"], info)
                        )
                        _node = self._db_session.query(Nodes).get(
                            info["sys_uid"])
                        _node.ipaddresses = json.dumps(
                            [ip] + json.loads(_node.ipaddresses)
                        )
                    else:
                        logger.info("find remote node host: %s" % ip)
                        logger.info(
                            "save node(%s) to database: %s" %
                            (info["sys_uid"], info)
                        )
                        _node = Nodes(
                            sys_uid=info["sys_uid"],
                            ipaddresses=json.dumps(info["ipaddresses"]),
                        )
                        self._db_session.add(_node)
                    self._db_session.commit()
                except SQLAlchemyError as e:
                    self._db_session.rollback()
                    logger.error(
                        "write node(%s) to database failed: %s" %
                        (info["sys_uid"], e)
                    )

    def _find_remote_nodes(self):
        logger.info("scan network segment")
        ipaddresses = []
        for ipaddress in get_local_interfaces():
            logger.info("ipaddress: %s" % ipaddress)
            ipaddresses += scan_port(
                    ipaddress["addr"], ipaddress["netmask"], self.rpc_port
            )
        else:
            logger.info("The scan is complete.")
        self._save_nodes(ipaddresses)

    def _create_to_repository(self, key, value):
        self.repository[key] = value

    @check_params
    @encrypt_result
    def create_kv(self, key, value):
        self._check_repository_ready()
        self._check_transaction_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.create_kv(key, value)
        else:
            self._check_srcr()
            if key in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.repository_transaction_id,
                "action": "_create_to_repository",
                "key": key,
                "value": value,
                "roll_action": "_delete_from_repository",
                "roll_value": None,
                "state": "ready",
            }
            _ac = self._save_transaction(**ac)
            if _ac:
                self.repository_transaction[ac["id"]] = ac
                self._repository_transaction_event.set()
                return True
            return False

    def _update_to_repository(self, key, value):
        self.repository[key] = value

    @check_params
    @encrypt_result
    def update_kv(self, key, value):
        self._check_repository_ready()
        self._check_transaction_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.update_kv(key, value)
        else:
            self._check_srcr()
            if key not in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.repository_transaction_id,
                "action": "_update_to_repository",
                "key": key,
                "value": value,
                "roll_action": "_update_to_repository",
                "roll_value": self._get_from_repository(key),
                "state": "ready",
            }
            _ac = self._save_transaction(**ac)
            if _ac:
                self.repository_transaction[ac["id"]] = ac
                self._repository_transaction_event.set()
                return True
            return False

    def _delete_from_repository(self, key):
        self.repository.pop(key)

    @check_params
    @encrypt_result
    def delete_kv(self, key):
        self._check_repository_ready()
        self._check_transaction_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.delete_kv(key)
        else:
            self._check_srcr()
            if key not in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.repository_transaction_id,
                "action": "_delete_from_repository",
                "key": key,
                "value": None,
                "roll_action": "_create_to_repository",
                "roll_value": None,
                "state": "ready",
            }
            _ac = self._save_transaction(**ac)
            if _ac:
                self.repository_transaction[ac["id"]] = ac
                self._repository_transaction_event.set()
                return True
            return False

    def _get_from_repository(self, key, prefix=False):
        if prefix:
            return {
                _key: self.repository[_key] for _key in self.repository
                if _key.startswith(key)
            }
        return self.repository[key]

    @check_params
    @encrypt_result
    def get_kv(self, key, prefix=False):
        self._check_repository_ready()
        self._check_transaction_ready()
        if self.role == 1:
            RuntimeError("Node is not ready")
        elif self.role == 2:
            return self._leader.get_kv(key, prefix=prefix)
        else:
            self._check_srcr()
            return self._get_from_repository(key, prefix)

    @check_params
    @encrypt_result
    def get_repository_transaction(self, id):
        self._check_transaction_ready()
        return self.repository_transaction[id]

    @check_params
    @encrypt_result
    def append_repository_transaction(self, **transaction):
        self._check_transaction_ready()
        if self.repository_transaction_id - 1 < transaction["id"]:
            if self.role == 2:
                _ac = self._save_transaction(**transaction)
                if _ac:
                    logger.debug(transaction)
                    self.repository_transaction[transaction["id"]] = transaction
                    self._repository_transaction_event.set()
                else:
                    raise RuntimeError("save transaction failed.")
            elif self.role == 0:
                CONF["_%s_ready" % __name__] = False
                self._leader = None
                self.role = 1
                self.have_voted = False
            return True
        elif self.repository_transaction_id - 1 == transaction["id"]:
            CONF["_%s_ready" % __name__] = False
            self._leader = None
            self.role = 1
            self.have_voted = False
            return False
        else:
            return False

    def _save_transaction(self, **kwargs):
        logger.debug("save transaction:\n%s" % kwargs)
        try:
            ac = Transaction(**kwargs)
            self._db_session.add(ac)
            self._db_session.commit()
            return ac
        except Exception as e:
            logger.error("save transaction failed: \n%s\n%s" % (kwargs, e))
            return False

    @check_params
    @encrypt_result
    def update_transaction(self, **transaction):
        self._check_transaction_ready()
        logger.debug("Update transaction: %s" % transaction)
        if transaction["id"] in self.repository_transaction:
            self.repository_transaction[transaction["id"]] = transaction
            self._repository_transaction_event.set()
            return True
        else:
            return False

    def _update_transaction(self, **transaction):
        logger.debug("new:\n%s" % transaction)
        try:
            ac = self._db_session.query(Transaction).get(transaction["id"])
            if not ac:
                logger.error("transaction not found: %s" % transaction)
                return False
            logger.debug("update transaction, old:\n%s" % ac.__dict__)
            for key, value in transaction.items():
                if getattr(ac, key) != value:
                    setattr(ac, key, value)
            self._db_session.commit()
            self.repository_transaction[transaction["id"]] = transaction
            self._repository_transaction_event.set()
            return True
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error(
                "update transaction failed:\n%s\n%s" % (transaction, e)
            )
            raise e

    def _delete_transaction(self, transaction):
        ac = self._db_session.query(Transaction).get(transaction["id"])
        if ac:
            logger.debug("transaction delete: %s" % ac.__dict__)
            self._db_session.delete(ac)
            self._db_session.commit()
        else:
            logger.debug("transaction not exists: %s" % transaction)

    def _rollback_transaction(self, transaction):
        if transaction["roll_action"] != "_delete_from_repository":
            args = (transaction["key"], transaction["roll_value"])
        else:
            args = (transaction["key"],)
        getattr(self, transaction["roll_action"])(*args)

    def _sync_transaction(self):
        self._repository_transaction_event.clear()
        leader_info = self._leader.info()
        logger.info("Check for differences ......")
        for tid in xrange(0, leader_info["transaction_id"]):
            ta = self._leader.get_repository_transaction(tid)
            if self.repository_transaction.get(tid, None) != ta:
                roll_id = tid
                break
        else:
            roll_id = self.repository_transaction_id
        logger.info("rollback id: %s" % roll_id)
        tid = self.repository_transaction_id - 1
        while tid >= roll_id:
            if tid in self.repository_transaction:
                ta = self.repository_transaction[tid]
                self._rollback_transaction(ta)
                self._delete_transaction(ta)
            tid -= 1
        logger.info("Transaction rollback completed.")
        for tid in xrange(roll_id, leader_info["transaction_id"]):
            ta = self._leader.get_repository_transaction(tid)
            if ta["state"] in ("committed", "failed"):
                self._save_transaction(**ta)
                self.repository_transaction[tid] = ta
        logger.info("Transaction saved.")
        return True

    def _watch_kv(self, ):
        """
        初始化时，先从数据库读取保存的事务日志。

        顺序获取每一条事务，
        状态为 failed 的事务直接跳过，
        状态为 ready 的事务，
         如果该节点是follower节点，则一直等待 leader 节点通知该事务可以执行，
         如果是leader节点则先同步该事务到 follower 节点，同步超过一半后，
         更改该事务状态为doing。
        状态为 doing 的事务直接执行该事务，并更改该事务状态为 committed。
         如果该节点为leader节点则通知follower节点更新该事务的状态为doing。
        """
        logger.info("watch kv")
        while 1:
            if not self._repository_transaction_ready:
                sleep(self.timeout)
                continue
            if self._repository_transaction_daemon_exit:
                logger.warning("repository transaction daemon exit.")
                return
            self._repository_transaction_event.wait()
            logger.debug("Get transaction event.")
            try:
                ac = deepcopy(
                    self.repository_transaction[self.repository_transaction_id]
                )
                logger.debug("transaction : %s" % ac)
                self._repository_ready = False
            except KeyError:
                logger.debug("No new transactions yet.")
                self._repository_transaction_event.clear()
                continue
            sync_votes = (len(self.remote_nodes.keys()) + 1) / 2.0
            if ac["state"] == "failed":
                logger.warning("Transaction is in a failed state: \n%s" % ac)
                self.repository_transaction_id += 1
                self._repository_ready = True

            elif ac["state"] == "ready":
                logger.debug("Transaction is a ready state: \n%s" % ac)
                if self.role == 0:
                    success = 1
                    for uid, rpc in self.remote_nodes.items():
                        if not rpc:
                            logger.warning(
                                "node(%s) is offline and cannot add "
                                "transactions." % uid
                            )
                            continue
                        try:
                            logger.debug(
                                "node(%s) will add transactions: \n%s" %
                                (uid, ac)
                            )
                            if rpc.append_repository_transaction(**ac):
                                success += 1
                            else:
                                logger.error(
                                    "The transaction on node(%s) "
                                    "is relatively new" % uid
                                )
                                self._repository_transaction_ready = False
                                self._repository_transaction_event.clear()
                                CONF["_%s_ready" % __name__] = False
                                self._leader = None
                                self.role = 1
                                self.have_voted = False
                                break
                        except Exception as e:
                            logger.error(
                                "node(%s) failed to add a transaction: \n%s" %
                                (uid, e)
                            )
                            if isinstance(e, RemoteError) and \
                                    "transaction is not ready" in e.msg:
                                logger.error(
                                    "The transaction will not execute."
                                )
                                break
                    else:
                        if success >= sync_votes:
                            logger.debug(
                                "The transaction will execute: \n%s" % ac
                            )
                            ac["state"] = "doing"
                            self._update_transaction(**ac)
                            self._repository_ready = True
                            logger.debug("Transaction synchronization success.")
                        else:
                            logger.error("Transaction synchronization failure.")
                        continue

                elif self.role == 2:
                    logger.debug("transaction event: clear.")
                    self._repository_transaction_event.clear()
                    self._repository_ready = True

            elif ac["state"] == "doing":
                logger.debug("Execute the transaction: \n%s" % ac)
                ac["roll_value"] = self.repository.get(ac["key"], None)
                if ac["action"] != "_delete_from_repository":
                    args = (ac["key"], ac["value"])
                else:
                    args = (ac["key"],)
                try:
                    getattr(self, ac["action"])(*args)
                    ac["state"] = "committed"
                except Exception as e:
                    ac["state"] = "failed"
                    logger.error("Transaction execution failre: \n%s" % e)
                    try:
                        self._rollback_transaction(ac)
                    except Exception as e:
                        logger.error("Rollback transaction failre: \n%s" % e)
                finally:
                    self._update_transaction(**ac)

                if self.role == 0:
                    for uid, rpc in self.remote_nodes.items():
                        if not rpc:
                            logger.warning(
                                "node(%s) is offline and cannot execute "
                                "transactions." % uid
                            )
                            continue
                        try:
                            logger.debug(
                                "node(%s) synchronizes update the "
                                "transactions: \n%s" % (uid, ac)
                            )
                            if ac["state"] == "committed":
                                ac["state"] = "doing"
                            rpc.update_transaction(**ac)
                            logger.debug(
                                "Transaction synchronization update success.")
                        except Exception as e:
                            logger.error(
                                "node(%s) failed to synchronizes "
                                "the transaction: \n%s" % (uid, e)
                            )

                self._repository_ready = True

            elif ac["state"] == "committed":
                logger.debug("Transaction is committed: %s" % ac)
                self.repository_transaction_id += 1
                self._repository_ready = True

    def _watch_cluster(self, ):
        logger.info("connect remote nodes.")
        if self._config.get("nodes", []):
            ipaddresses = self._config.get("nodes")
            self._save_nodes(ipaddresses)
            self._join_remote_nodes()
        if self._config.get("scan", False):
            self._config["scan"] = False
            CONF[__package__] = self._config
            self._find_remote_nodes()
            self._join_remote_nodes()
        logger.info("remote nodes: %s" % self.remote_nodes)
        logger.info("watch cluster")
        keepalive_g_stat = dict()
        while 1:
            if self._cluster_daemon_exit:
                logger.warning("cluster daemon exit.")
                return
            if self.join_nodes:
                self._task_pool.spawn(self._check_join_node)

            if self.role == 0:
                for uid in self.remote_nodes:
                    if getattr(keepalive_g_stat.get(uid, None), "dead", True):
                        keepalive_g_stat[uid] = self._task_pool.spawn(
                            self._send_keepalive, uid
                        )
                sleep(self.heartbeat)

            elif self.role == 1:
                if not self._election():
                    logger.warning("sleep %s" % self.timeout)
                    sleep(self.timeout)
                else:
                    logger.info("my role: %s" % SRkvNodeRole[self.role])

            elif self.role == 2:
                if not self._repository_transaction_ready:
                    try:
                        logger.info(
                            "Starts synchronizing transactions from leader."
                        )
                        _g = self._task_pool.spawn(self._sync_transaction)
                        _g.join()
                        logger.info("Transaction synchronization success!")
                        CONF["_%s_ready" % __name__] = True
                        self._repository_transaction_ready = True
                        self._repository_ready = True
                    except RemoteError as e:
                        logger.error(
                            "Transaction synchronization failed: \n%s" % e
                        )
                if not self.leader_keepalive:
                    self._srcr_status["ready"].pop(self.leader_uid)
                    logger.error("leader(%s) is offline!" % self.leader_uid)
                    CONF["_%s_ready" % __name__] = False
                    self._leader = None
                    self.role = 1
                    self.have_voted = False
                    self._repository_transaction_ready = True
                    self._repository_ready = False
                else:
                    self.leader_keepalive = False
                    sleep(self.timeout)

    def run(self, ):
        logger.info("Load transaction.")
        for ac in self._db_session.query(Transaction).order_by(
                Transaction.id
        ).options(lazyload("*")):
            action = {
                "id": ac.id,
                "action": ac.action,
                "key": ac.key,
                "value": ac.value,
                "roll_action": ac.roll_action,
                "roll_value": ac.roll_value,
                "state": ac.state
            }
            self.repository_transaction[ac.id] = action
            logger.debug("transaction: %s" % ac.__dict__)
            if ac.state == "committed":
                if ac.action != "_delete_from_repository":
                    args = (ac.key, ac.value)
                else:
                    args = (ac.key,)
                try:
                    getattr(self, ac.action)(*args)
                except Exception as e:
                    logger.error("transaction(%s) exception: %s" % (ac.id, e))
            if ac.state == "doing":
                action["state"] = "failed"
                self._update_transaction(**action)
            self.repository_transaction_id = ac.id + 1
        self._repository_transaction_ready = True

        self._cluster_daemon_exit = 0
        self._cluster_daemon = spawn(self._watch_cluster)
        self._repository_transaction_daemon = spawn(self._watch_kv)
        self._repository_transaction_daemon_exit = 0
        self._acceptor_task = spawn(self._acceptor)
        while 1:
            sleep(1)
            if self._acceptor_task.ready():
                _exc_info = "".join(
                    format_exception(*self._acceptor_task.exc_info)
                )
                if _exc_info:
                    logger.error(
                        "acceptor task exits unexpectedly: %s" % _exc_info
                    )
                else:
                    logger.info("acceptor task exit.")
                break
            if self._cluster_daemon.ready():
                _exc_info = "".join(
                    format_exception(*self._cluster_daemon.exc_info)
                )
                if _exc_info:
                    logger.error(
                        "cluster_daemon task exits unexpectedly: %s" % _exc_info
                    )
                else:
                    logger.info("cluster_daemon exit.")
                break
            if self._repository_transaction_daemon.ready():
                _exc_info = "".join(
                    format_exception(
                        *self._repository_transaction_daemon.exc_info
                    )
                )
                if _exc_info:
                    logger.error(
                        "repository_transaction_daemon exits unexpectedly: %s"
                        % _exc_info
                    )
                else:
                    logger.info("repository transaction daemon exit.")
                break
        self.stop()
        self._task_pool.join(raise_error=True)

    def stop(self, ):
        logger.warning("node server stop.")
        if self._repository_transaction_daemon:
            self._repository_transaction_daemon_exit = 1
            self._repository_transaction_daemon.join()
        if self._cluster_daemon:
            self._cluster_daemon_exit = 1
            self._cluster_daemon.join()
        if self._acceptor_task is not None:
            self._acceptor_task.kill()
            self._acceptor_task = None


def proc():
    my_pid = os.getpid()
    config = CONF.get(__package__)
    node = Node(config)
    port = config.get("port")
    node.bind("tcp://0.0.0.0:%s" % port)
    node.bind("ipc://%s" % SRkvLocalSock)

    def exit_proc(signum=None, frame=None):
        if os.getpid() == my_pid:
            logger.warning("server exit")
            node.stop()
            node.close()
        return

    signal.signal(signal.SIGINT, exit_proc)
    logger.info("SRkv server start")
    server = spawn(node.run)
    server.join()
