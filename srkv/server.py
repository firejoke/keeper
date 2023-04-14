# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2022/5/19 17:23
import json
import sys
from traceback import format_exception

import signal
from collections import Iterator, Mapping, OrderedDict, Sequence, Set
from functools import wraps
from logging import getLogger
from uuid import uuid4

import os
from copy import deepcopy, copy

from gevent import sleep, spawn, Greenlet, wait
from gevent.event import Event as gEvent
from gevent.hub import signal as g_signal
from random import uniform

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import lazyload
from time import time
from zerorpc import (
    LostRemote, RemoteError, Server, TimeoutExpired, stream as z_stream
)

from models import Nodes, Repository, Transaction
from public_def import (
    CONF, RpcClient, SRkvLocalSock, SRkvNodeRole, SRkvTransactionLogSize,
    SRkvTransactionLoadSize, db_url,
    decrypt_text, encrypt_obj, get_local_interfaces, ip_check, root_path,
    scan_port,
)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


db_engine = create_engine(db_url)
Session = sessionmaker(bind=db_engine)
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

    uuid_path = os.path.join(root_path, ".sys_uid")
    if not os.path.exists(uuid_path):
        sys_uid = str(uuid4())
        with open(uuid_path, "w") as f:
            f.write(sys_uid)
    else:
        with open(uuid_path) as f:
            sys_uid = f.read().strip()

    def __init__(self, config):
        self._config = config
        self.ipaddresses = list(
            self.ipaddresses - set(self._config.get("exclude_ipaddress", []))
        )
        self.cluster_name = self._config["cluster_name"]
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
        self.join_nodes = dict()
        self._check_join_node_status = False
        self._srcr_status = {"me": self.sys_uid, "ready": dict(), "nodes": []}
        self._db_session = Session()
        self.repository = dict()
        self._repository_ready = False
        self._repository_daemon = None
        self.transaction = OrderedDict()
        # 已执行的事务编号
        self.transaction_id = 0
        self._transaction_ready = False
        self._transaction_event = gEvent()
        self._transaction_daemon = None
        self._cluster_daemon = None
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

    def _check_role(self, expected_role):
        if self.role == 1:
            raise RuntimeError("my role is %s" % SRkvNodeRole[self.role])
        elif expected_role != self.role:
            logger.error(
                "The local node expects its role to be %s, but %s"
                % (SRkvNodeRole[expected_role], SRkvNodeRole[self.role])
            )
            CONF["_%s_ready" % __name__] = False
            self._leader = None
            self.role = 1
            self.have_voted = False
            return False
        else:
            return True

    def _check_transaction_ready(self):
        now = time()
        while not self._transaction_ready:
            if time() - now <= self.base_timeout:
                sleep(0.1)
            else:
                raise RuntimeError("transaction is not ready.")
        else:
            return True

    def _check_repository_ready(self):
        now = time()
        while not self._repository_ready:
            if time() - now <= self.base_timeout:
                sleep(0.1)
            else:
                raise RuntimeError("repository is not ready.")

    def _check_sr_cluster_ready(self, ):
        if not all(self._srcr_status["ready"].values()):
            raise RuntimeError("srkv cluster is not ready.")

    @check_params
    @encrypt_result
    def info(self):
        # self._check_transaction_ready()
        return {
            "sys_uid": self.sys_uid,
            "leader": self.leader_uid,
            "role": self.role,
            "ipaddresses": self.ipaddresses,
            "term_counter": self.term_counter,
            "have_voted": self.have_voted,
            "votes": self.votes,
            "remote_nodes": self.remote_nodes.keys(),
            "transaction_id": self.transaction_id,
            "transaction_ready": self._transaction_ready,
            "cluster_name": self.cluster_name,
            "repository_ready": self._repository_ready
        }

    @check_params
    @encrypt_result
    def keepalive(self, leader_uid, leader_transaction_id):
        if leader_uid == self.leader_uid:
            self.leader_keepalive = True
            if leader_transaction_id > self.transaction_id:
                self._transaction_ready = False
                CONF["_%s_ready" % __name__] = False
            return leader_uid
        elif self.role == 1:
            return None
        elif (
                self.role == 2 and leader_uid != self.leader_uid and
                leader_transaction_id > self._leader.info()["transaction_id"]
        ) or (
                self.role == 0 and
                leader_transaction_id > self.transaction_id
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
                logger.warning("try connect node(%s)" % uid)
                node = self._db_session.query(Nodes).get(uid)
                c, info = self._connect_node(json.loads(node.ipaddresses))
                if c:
                    self.remote_nodes[uid] = c
                    self._srcr_status["ready"][uid] = c.info()[
                        "transaction_ready"]
                else:
                    logger.error("connection to node(%s) failed" % uid)
            else:
                node_leader = self.remote_nodes[uid].keepalive(
                    self.sys_uid, self.transaction_id
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
                    try:
                        self._srcr_status["ready"][uid] = self.remote_nodes[
                            uid].info()["transaction_ready"]
                    except RemoteError as e:
                        logger.warning(
                            "Failed to send a heartbeat to node(%s): %s"
                            % (uid, e)
                        )
        except (LostRemote, TimeoutExpired) as e:
            self.remote_nodes[uid].close()
            self.remote_nodes[uid] = None
            if uid in self._srcr_status:
                self._srcr_status["ready"][uid] = False
            logger.error("connect node(%s) failed: \n%s" % (uid, e))
        return

    @check_params
    @encrypt_result
    def vote(self, node_uid, node_term_counter, transaction_id):
        if self.role == 2:
            _transaction_max_id = self._leader.info()["transaction_id"]
        else:
            _transaction_max_id = self.transaction_id
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
            logger.info("My leader: %s" % self.leader_uid)
            logger.info(
                "My role changes from %s to follower" % SRkvNodeRole[self.role]
            )
            self.role = 2
            self.votes[node_uid] = 1
            self._repository_ready = False
            self._transaction_ready = False
            CONF["_%s_ready" % __name__] = False
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
            if not self.remote_nodes[_uid]:
                logger.info("The node(%s) is closed;")
                self.remote_nodes[_uid] = None
                continue
            logger.info("canvass votes for node(%s)" % _uid)
            try:
                node_leader = self.remote_nodes[_uid].vote(
                    self.sys_uid, self.term_counter,
                    self.transaction_id
                )
                logger.info("node(%s) of leader: %s" % (_uid, node_leader))
                if node_leader in self.votes:
                    self.votes[node_leader] += 1
                elif node_leader:
                    self.votes[node_leader] = 1
            except (LostRemote, TimeoutExpired) as e:
                logger.error(
                    "node(%s) connect failed:\n%s" % (_uid, e)
                )
                self.remote_nodes[_uid].close()
                self.remote_nodes[_uid] = None
                if _uid in self._srcr_status:
                    self._srcr_status["ready"][_uid] = False
            except RemoteError as e:
                logger.error("failed to canvass for node(%s): \n%s" % (_uid, e))
                return False
        return True

    def _election(self):
        logger.info("My uid: %s" % self.sys_uid)
        if self.role == 1:
            logger.info("start election")
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
                self._transaction_ready = True
                CONF["_%s_ready" % __name__] = True
            else:
                self.role = 2
                self._leader = self.remote_nodes[self.leader_uid]
                self.leader_keepalive = True
                self._repository_ready = False
                self._transaction_ready = False

        return True

    @check_params
    @encrypt_result
    def echo(self, node_uid, node_ipaddress, cluster_name):
        if cluster_name != self.cluster_name:
            raise RuntimeError("The cluster names do not match.")
        if node_uid != self.sys_uid \
                and not self.remote_nodes.get(node_uid, None)\
                and not self.join_nodes.get(node_uid, None):
            self.join_nodes[node_uid] = node_ipaddress

        return {"sys_uid": self.sys_uid, "ipaddresses": self.ipaddresses}

    def _check_join_node(self):
        self._check_join_node_status = True
        check_nodes = []
        for node_uid, node_ips in self.join_nodes.items():
            if node_uid != self.sys_uid \
                    and not self.remote_nodes.get(node_uid, None):
                logger.warning(
                    "try connect node(%s, %s)" % (node_uid, node_ips)
                )
                check_nodes.append(
                    self._task_pool.spawn(
                        self._connect_node, node_ips
                    )
                )
        sleep(self.heartbeat)
        if check_nodes:
            logger.info("Check the added nodes.")
        for _node in check_nodes:
            c, info = _node.get()
            if c:
                self.remote_nodes[info["sys_uid"]] = c
                self._srcr_status["ready"][info["sys_uid"]] = c.info()[
                    "transaction_ready"]
                self.join_nodes.pop(info)
                if not self._db_session.query(Nodes).get(info["sys_uid"]):
                    try:
                        logger.info("save node(%s) to database: %s" %
                                    (info["sys_uid"], info))
                        node = Nodes(
                            sys_uid=info["sys_uid"],
                            ipaddresses=json.dumps(info["ipaddresses"])
                        )
                        self._db_session.add(node)
                        self._db_session.commit()
                    except SQLAlchemyError as e:
                        logger.error(
                            "node(%s) save failed: \n%s" % (info["sys_uid"], e)
                        )
                    finally:
                        self._db_session.rollback()
        self._check_join_node_status = False

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
                logger.error("connect node(%s) failed:\n%s" % (ip, e))
                continue
            else:
                logger.info("connect node(%s) success:\n%s" % (ip, info))
                return c, info
        c.close()
        return None, None

    def _join_remote_nodes(self):
        nodes = [
            self._task_pool.spawn(
                self._connect_node, json.loads(node.ipaddresses)
            )
            for node in self._db_session.query(Nodes).filter(
                Nodes.sys_uid != self.sys_uid
            ).all()
            if not self.remote_nodes.get(node.sys_uid, None)
        ]
        sleep(self.heartbeat)
        for node in nodes:
            c, info = node.get()
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
        nodes = [
            self._task_pool.spawn(self._connect_node, [ip])
            for ip in ipaddresses
            if ip_check(ip) and ip not in exists_ips
        ]
        sleep(self.heartbeat)
        for node in nodes:
            c, info = node.get()
            if c:
                try:
                    if info["sys_uid"] in exists_uids:
                        _node = self._db_session.query(Nodes).get(
                            info["sys_uid"])
                        if _node.ipaddresses == info["ipaddresses"]:
                            continue
                        logger.info(
                            "update node(%s): %s" % (info["sys_uid"], info)
                        )
                        _node.ipaddresses = json.dumps(info["ipaddresses"])
                    else:
                        logger.info("find remote node host: %s" % info)
                        logger.info(
                            "save node(%s) to database: %s" %
                            (info["sys_uid"], info)
                        )
                        _node = Nodes(
                            sys_uid=info["sys_uid"],
                            ipaddresses=json.dumps(info["ipaddresses"]),
                        )
                        self._db_session.add(_node)
                        exists_uids.append(info["sys_uid"])
                    self._db_session.commit()
                except SQLAlchemyError as e:
                    self._db_session.rollback()
                    logger.error(
                        "write node(%s) to database failed: %s" %
                        (info["sys_uid"], e)
                    )
                finally:
                    c.lose()

    def _find_remote_nodes(self):
        logger.info("scan network segment")
        ipaddresses = []
        exclude_ipaddresses = self._config.get("exclude_ipaddresses", [])
        for ipaddress in get_local_interfaces(
            self._config.get("require_gateway", True)
        ):
            if ipaddress["addr"] in exclude_ipaddresses:
                continue
            logger.info("ipaddress: %s" % ipaddress)
            ipaddresses += scan_port(
                ipaddress["addr"], ipaddress["netmask"], self.rpc_port,
                exclude_ipaddresses
            )
        else:
            logger.info("The scan is complete.")
        self._save_nodes(ipaddresses)

    def _create_to_repository(self, key, value):
        self.repository[key] = self._save_repository(key, value)

    @check_params
    @encrypt_result
    def create_kv(self, key, value):
        self._check_repository_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.create_kv(key, value)
        else:
            self._check_transaction_ready()
            if key in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.transaction_id + 1,
                "action": "_create_to_repository",
                "key": key,
                "value": value,
                "roll_action": "_delete_from_repository",
                "roll_value": None,
                "state": "ready",
            }
            ac = self._task_pool.spawn(self._save_transaction, **ac).get()
            if ac:
                self.transaction[ac["id"]] = ac
                self._transaction_event.set()
                return True
            return False

    def _update_to_repository(self, key, value):
        self.repository[key] = self._update_repository(key, value)

    @check_params
    @encrypt_result
    def update_kv(self, key, value):
        self._check_repository_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.update_kv(key, value)
        else:
            self._check_transaction_ready()
            if key not in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.transaction_id + 1,
                "action": "_update_to_repository",
                "key": key,
                "value": value,
                "roll_action": "_update_to_repository",
                "roll_value": self._get_from_repository(key),
                "state": "ready",
            }
            ac = self._task_pool.spawn(self._save_transaction, **ac).get()
            if ac:
                self.transaction[ac["id"]] = ac
                self._transaction_event.set()
                return True
            return False

    def _delete_from_repository(self, key):
        self._delete_repository(key)
        self.repository.pop(key)

    @check_params
    @encrypt_result
    def delete_kv(self, key):
        self._check_repository_ready()
        if self.role == 1:
            RuntimeError("Node not ready.")
        elif self.role == 2:
            return self._leader.delete_kv(key)
        else:
            self._check_transaction_ready()
            if key not in self.repository:
                return False
            self._repository_ready = False
            ac = {
                "id": self.transaction_id + 1,
                "action": "_delete_from_repository",
                "key": key,
                "value": None,
                "roll_action": "_create_to_repository",
                "roll_value": None,
                "state": "ready",
            }
            ac = self._task_pool.spawn(self._save_transaction, **ac).get()
            if ac:
                self.transaction[ac["id"]] = ac
                self._transaction_event.set()
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
        if self.role == 1:
            RuntimeError("Node is not ready")
        elif self.role == 2:
            return self._leader.get_kv(key, prefix=prefix)
        else:
            self._check_transaction_ready()
            return self._get_from_repository(key, prefix)

    @z_stream
    @check_params
    @encrypt_stream
    def get_repository(self):
        return self.repository

    def _get_repository_from_leader(self):
        self._repository_ready = False
        old_keys = self.repository.keys()
        for k, v in self._task_pool.spawn(self._leader.get_repository).get():
            if k in old_keys:
                old_keys.remove(k)
                self.repository.update({k: v})
                self._task_pool.spawn(self._update_repository, k, v).join()
            elif k not in old_keys:
                self.repository[k] = v
                self._task_pool.spawn(self._save_repository, k, v).join()
        for k in old_keys:
            self.repository.pop(k)
            self._task_pool.spawn(self._delete_repository, k).join()
        self._repository_ready = True

    def _save_repository(self, key, value):
        logger.debug("save repository:\n%s: %s" % (key, value))
        try:
            kv = Repository(key=key, value=value)
            self._db_session.add(kv)
            self._db_session.commit()
            return value
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error("save repository(%s=%s) failed:\n%s" % (key, value, e))
            raise e

    def _update_repository(self, key, value):
        logger.debug("update repository:\n%s: %s" % (key, value))
        try:
            kv = self._db_session.query(Repository).get(key)
            if not kv:
                logger.error("kv(%s) not found." % key)
                return False
            old_kv = {key: kv.value}
            logger.debug("update repository, old:\n%s" % old_kv)
            kv.value = value
            self._db_session.commit()
            return value
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error(
                "update repository(%s=%s) failed:\n%s" % (key, value, e)
            )
            raise e

    def _delete_repository(self, key):
        logger.debug("delete %s from repository" % key)
        try:
            kv = self._db_session.query(Repository).get(key)
            if kv:
                self._db_session.delete(kv)
                self._db_session.commit()
                return True
            else:
                logger.error("%s not in repository." % key)
                return False
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error("delete %s from repository failed:\n%s" % (key, e))

    @check_params
    @encrypt_result
    def get_transaction_uuid(self, t_id):
        if self.role != 0:
            self._check_repository_ready()
        return self.transaction[t_id]["uuid"]

    @check_params
    @encrypt_result
    def get_transaction(self, t_id):
        if self.role != 0:
            self._check_transaction_ready()
        return self.transaction[t_id]

    @check_params
    @encrypt_result
    def append_transaction(self, **transaction):
        if not self._check_role(2):
            return False
        self._check_transaction_ready()
        if transaction["id"] - self.transaction_id > 1:
            CONF["_%s_ready" % __name__] = False
            self._transaction_ready = False
            logger.error(
                "Transaction id: %d, local transaction id: %d" %
                (transaction["id"], self.transaction_id)
            )
            raise RuntimeError("Local transactions are not synchronized.")

        elif transaction["id"] - self.transaction_id == 1:
            _ac = self._task_pool.spawn(
                self._save_transaction, **transaction
            ).get()
            if _ac:
                logger.debug(
                    "Append transaction from leader:\n%s" % transaction
                )
                self.transaction[transaction["id"]] = transaction
                self._transaction_event.set()
                return True
            else:
                raise RuntimeError("save transaction failed.")
        elif transaction["id"] - self.transaction_id < 1:
            return False

    def _save_transaction(self, **transaction):
        try:
            if "uuid" not in transaction:
                transaction.update(uuid=uuid4().hex)
            logger.debug("save transaction:\n%s" % transaction)
            ac = self._db_session.query(Transaction).get(transaction["id"])
            if not ac:
                ac = Transaction(**transaction)
                self._db_session.add(ac)
                self._db_session.commit()
            return transaction
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error("save transaction failed:\n%s\n%s" % (transaction, e))
            raise e

    @check_params
    @encrypt_result
    def update_transaction(self, **transaction):
        if not self._check_role(2):
            return False
        self._check_transaction_ready()
        if transaction["id"] < self.transaction_id + 1:
            return False
        elif transaction["id"] == self.transaction_id + 1:
            if transaction["uuid"] == \
                    self.transaction[transaction["id"]]["uuid"]:
                logger.debug("Update transaction: %s" % transaction)
                self.transaction[transaction["id"]] = transaction
                self._task_pool.spawn(
                    self._update_repository, **transaction
                ).join()
                return True
            else:
                return False
        else:
            self._transaction_ready = False
            CONF["_%s_ready" % __name__] = False
            logger.debug(
                "Transaction id: %d, local transaction id: %d" %
                (transaction["id"], self.transaction_id)
            )
            raise RuntimeError("Local transactions are not synchronized")

    def _update_transaction(self, **transaction):
        logger.debug("new:\n%s" % transaction)
        try:
            ac = self._db_session.query(Transaction).get(transaction["id"])
            if not ac:
                logger.error("transaction not found: %s" % transaction)
                return False
            old_transaction = {
                "id": ac.id,
                "action": ac.action,
                "key": ac.key,
                "value": ac.value,
                "roll_action": ac.roll_action,
                "roll_value": ac.roll_value,
                "state": ac.state,
                "uuid": ac.uuid,
            }
            logger.debug("update transaction, old:\n%s" % old_transaction)
            for key, value in transaction.items():
                if old_transaction[key] != value:
                    setattr(ac, key, value)
            self._db_session.commit()
            self.transaction[transaction["id"]] = transaction
            self._transaction_event.set()
            return True
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error(
                "update transaction failed:\n%s\n%s" % (transaction, e)
            )
            raise e

    def _delete_transaction(self, transaction_id):
        logger.debug("delete transaction(%s)" % transaction_id)
        try:
            ac = self._db_session.query(Transaction).get(transaction_id)
            if ac:
                self._db_session.delete(ac)
                self._db_session.commit()
                return True
            else:
                logger.debug("transaction(%s) not exists" % transaction_id)
                return False
        except SQLAlchemyError as e:
            self._db_session.rollback()
            logger.error("Transaction delete failed:\n%s" % e)

    def _rollback_transaction(self, transaction):
        if transaction["roll_action"] != "_delete_from_repository":
            args = (transaction["key"], transaction["roll_value"])
        else:
            args = (transaction["key"],)
        try:
            getattr(self, transaction["roll_action"])(*args)
        except Exception as e:
            logger.error("Transaction rollback failed: %s" % e)

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
            self._transaction_event.wait()
            if not self._transaction_ready:
                sleep(1)
                continue
            logger.debug("Get transaction event.")
            try:
                self._repository_ready = False
                ac = deepcopy(
                    self.transaction[
                        self.transaction_id + 1]
                )
                logger.debug("transaction : %s" % ac)
            except KeyError:
                self._repository_ready = True
                logger.debug(
                    "No new transactions yet, id %d is expected."
                    % (self.transaction_id + 1)
                )
                self._transaction_event.clear()
                continue
            sync_votes = (
                len([node for node, rpc in self.remote_nodes.items() if rpc])
                + 1
            ) / 2.0

            if ac["state"] == "ready":
                logger.debug("Transaction is a ready state: \n%s" % ac)
                if self.role == 0:
                    success = 1
                    trans = dict()
                    for uid, rpc in self.remote_nodes.items():
                        if not rpc:
                            logger.warning(
                                "node(%s) is offline and cannot add "
                                "transactions." % uid
                            )
                            continue
                        logger.debug(
                            "node(%s) will add transactions: \n%s" %
                            (uid, ac)
                        )
                        trans[uid] = self._task_pool.spawn(
                            rpc.append_transaction, **ac
                        )
                    for uid, g in trans.items():
                        try:
                            if g.get():
                                success += 1
                            else:
                                logger.error(
                                    "The transaction on node(%s) "
                                    "is relatively new" % uid
                                )
                                self.role = 1
                                CONF["_%s_ready" % __name__] = False
                                self._leader = None
                                self.have_voted = False
                                self._transaction_event.clear()
                        except Exception as e:
                            logger.error(
                                "node(%s) failed to add a transaction: \n%s" %
                                (uid, e)
                            )
                            if isinstance(e, RemoteError):
                                logger.error(
                                    "The transaction will not execute."
                                )
                    else:
                        if success >= sync_votes:
                            logger.debug(
                                "The transaction will execute: \n%s" % ac
                            )
                            ac["state"] = "doing"
                            self._task_pool.spawn(
                                self._update_transaction, **ac
                            ).join()
                            self._repository_ready = True
                            logger.debug("Transaction synchronization success.")
                        else:
                            logger.error("Transaction synchronization failure.")
                        continue

                elif self.role == 2:
                    logger.debug("transaction event: clear.")
                    self._transaction_event.clear()
                    # self._repository_ready = True

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
                        self._task_pool.spawn(
                            self._rollback_transaction, ac
                        ).join()
                    except Exception as e:
                        logger.error("Rollback transaction failre: \n%s" % e)
                finally:
                    self._task_pool.spawn(self._update_transaction, **ac).join()

            elif ac["state"] in ("committed", "failed"):
                logger.debug("Transaction is %s: %s" % (ac["state"], ac))
                if self.role == 0:
                    trans = dict()
                    for uid, rpc in self.remote_nodes.items():
                        if not rpc:
                            logger.warning(
                                "node(%s) is offline and cannot execute "
                                "transactions." % uid
                            )
                            continue
                        if ac["state"] == "committed":
                            ac["state"] = "doing"
                        logger.debug(
                            "node(%s) synchronizes update the "
                            "transactions: \n%s" % (uid, ac)
                        )
                        trans[uid] = self._task_pool.spawn(
                            rpc.update_transaction, **ac
                        )
                    for uid, g in trans.items():
                        try:
                            if not g.get():
                                self.role = 1
                                CONF["_%s_ready" % __name__] = False
                                self._leader = None
                                self.have_voted = False
                                self._transaction_event.clear()
                                break
                            logger.debug(
                                "Transaction synchronization update success."
                            )
                        except Exception as e:
                            logger.error(
                                "node(%s) failed to synchronizes "
                                "the transaction: \n%s" % (uid, e)
                            )
                    else:
                        self._repository_ready = True
                self.transaction_id = ac["id"]
                self._repository_ready = True

    def _watch_cluster(self, ):
        logger.info("watch cluster")
        if self._config.get("nodes", []):
            ipaddresses = self._config.get("nodes")
            self._task_pool.spawn(self._save_nodes, ipaddresses).join()
        if self._config.get("scan", False):
            self._config["scan"] = False
            CONF[__package__] = self._config
            self._task_pool.spawn(self._find_remote_nodes).join()
        keepalive_g_stat = dict()
        while 1:
            if self.join_nodes and not self._check_join_node_status:
                self._task_pool.spawn(self._check_join_node)

            if self.role == 0:
                for uid in self.remote_nodes:
                    if getattr(keepalive_g_stat.get(uid, None), "dead", True):
                        keepalive_g_stat[uid] = self._task_pool.spawn(
                            self._send_keepalive, uid
                        )
                sleep(self.heartbeat)

            elif self.role == 1:
                logger.info("connect remote nodes.")
                self._task_pool.spawn(self._join_remote_nodes).join()
                logger.info("remote nodes: %s" % self.remote_nodes)
                logger.warning("sleep %s" % self.timeout)
                sleep(self.timeout)
                if self._transaction_event.ready():
                    logger.info("Wait for the transaction to be ready.")
                    continue
                if not self._election():
                    logger.warning(
                        "Unsuccessful election, waiting for the next round"
                    )
                else:
                    logger.info("my role: %s" % SRkvNodeRole[self.role])

            elif self.role == 2:
                if not self.leader_keepalive:
                    self._repository_ready = False
                    self._leader = None
                    self._srcr_status["ready"][self.leader_uid] = False
                    self.remote_nodes[self.leader_uid].close()
                    self.remote_nodes[self.leader_uid] = None
                    self.role = 1
                    self.have_voted = False
                    self._transaction_ready = True
                    logger.error("leader(%s) is offline!" % self.leader_uid)
                    CONF["_%s_ready" % __name__] = False
                else:
                    self.leader_keepalive = False
                    sleep(self.timeout)

    def _review_transaction(self):
        logger.info("Review transaction.")
        while 1:
            if self.role == 2 and not self._transaction_ready:
                self._transaction_event.clear()
                logger.info("Starts synchronizing repository from leader.")
                self._task_pool.spawn(self._get_repository_from_leader).get()
                logger.info("Starts synchronizing transactions from leader.")
                leader_info = self._leader.info()
                ltid = leader_info["transaction_id"]
                if ltid < SRkvTransactionLoadSize:
                    ltid_s = 1
                else:
                    ltid_s = ltid - (SRkvTransactionLoadSize - 1)
                logger.info(
                    "Retrieves transactions with ids %d through %d "
                    "on the leader node" % (ltid_s, ltid + 1)
                )
                for tid in xrange(ltid_s, ltid + 1):
                    try:
                        lta = self._task_pool.spawn(
                            self._leader.get_transaction, tid
                        ).get()
                    except RemoteError as e:
                        if e.name == "KeyError":
                            logger.error(e.traceback)
                        lta = None
                    except Exception as e:
                        logger.error(
                            "Transaction synchronization failed: \n%s" % e
                        )
                        sleep(self.timeout)
                        break
                    ta = self.transaction.get(tid, None)
                    if ta != lta:
                        logger.info("transaction of leader: %s" % lta)
                        logger.info("transaction of local: %s" % ta)
                        if ta:
                            self._task_pool.spawn(
                                self._delete_transaction, tid
                            ).join()
                        if lta:
                            self._task_pool.spawn(
                                self._save_transaction, **lta
                            )
                        self.transaction[tid] = lta
                    self.transaction_id = tid
                else:
                    logger.info("Transaction synchronization success!")
                    self._transaction_ready = True
                    CONF["_%s_ready" % __name__] = True
            if not time() % (60 * 5):
                transaction_log_size = self._task_pool.spawn(
                    self._task_pool.spawn(
                        self._db_session.query, Transaction.id
                    ).get().count
                ).get()
                if transaction_log_size > SRkvTransactionLogSize:
                    logger.debug(
                        "transaction log size: %s" % transaction_log_size
                    )
                    min_id = self.transaction_id - SRkvTransactionLogSize
                    for ac in self._task_pool.spawn(
                            self._db_session.query, Transaction
                    ).get().filter(Transaction.id <= min_id):
                        self._delete_transaction(ac.id)
            sleep(1)

    def run(self, ):
        logger.info("Load repository.")
        for kv in self._db_session.query(Repository):
            logger.debug("load kv: {%s: %s}" % (kv.key, kv.value))
            self.repository.update({kv.key: kv.value})
        logger.info("Load transaction.")
        for ac in self._db_session.query(Transaction).order_by(
                Transaction.id
        ).options(lazyload("*"))[-SRkvTransactionLoadSize:]:
            transaction = {
                "id": ac.id,
                "action": ac.action,
                "key": ac.key,
                "value": ac.value,
                "roll_action": ac.roll_action,
                "roll_value": ac.roll_value,
                "state": ac.state,
                "uuid": ac.uuid,
            }
            self.transaction[ac.id] = transaction
            logger.debug("transaction: %s" % transaction)
            if ac.action != "_delete_from_repository":
                args = (ac.key, ac.value)
            else:
                args = (ac.key,)
            try:
                # if ac.state == "committed":
                #     getattr(self, ac.action)(*args)
                if ac.state in ("doing", "ready"):
                    transaction["state"] = "failed"
                    self._update_transaction(**transaction)
            except Exception as e:
                logger.error("transaction(%s) exception: %s" % (ac.id, e))
                raise e
            self.transaction_id = ac.id

        self._transaction_ready = True
        self._cluster_daemon = spawn(self._watch_cluster)
        self._repository_daemon = spawn(self._watch_kv)
        self._transaction_daemon = spawn(self._review_transaction)
        logger.debug("acceptor task start.")
        self._acceptor_task = spawn(self._acceptor)
        try:
            while 1:
                sleep(1)
                if self._acceptor_task.ready():
                    _exc_info = "".join(
                        format_exception(*self._acceptor_task.exc_info)
                    )
                    if _exc_info:
                        logger.error(
                            "acceptor task exits unexpectedly:\n%s" % _exc_info
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
                            "cluster_daemon task exits unexpectedly:\n%s"
                            % _exc_info
                        )
                    else:
                        logger.info("cluster_daemon exit.")
                    break
                if self._repository_daemon.ready():
                    _exc_info = "".join(
                        format_exception(*self._repository_daemon.exc_info)
                    )
                    if _exc_info:
                        logger.error(
                            "repository_daemon task exits unexpectedly:\n%s"
                            % _exc_info
                        )
                if self._transaction_daemon.ready():
                    _exc_info = "".join(
                        format_exception(
                            *self._transaction_daemon.exc_info
                        )
                    )
                    if _exc_info:
                        logger.error(
                            "transaction_daemon exits unexpectedly:\n%s"
                            % _exc_info
                        )
                    else:
                        logger.info("repository transaction daemon exit.")
                    break
        finally:
            self.stop()
            self._task_pool.join(raise_error=True)

    def stop(self, ):
        logger.warning("node server stop.")
        for name, rpc in self.remote_nodes.items():
            if rpc is not None:
                logger.info("rpc of %s close." % name)
                rpc.close()
                self.remote_nodes.pop(name)

        try:
            self._db_session.close()
        except Exception:
            logger.error("\n%s" % "".join(format_exception(*sys.exc_info())))

        for g in (
                "_acceptor_task", "_cluster_daemon", "_repository_daemon",
                "_transaction_daemon"
        ):
            gi = getattr(self, g)
            if gi is not None:
                try:
                    gi.kill()
                except Exception:
                    logger.error(
                        "\n%s" % "".join(format_exception(*sys.exc_info()))
                    )
                setattr(self, g, None)
        return


def proc():
    my_pid = os.getpid()
    config = CONF.get(__package__)
    node = Node(config)
    port = config.get("port")
    node.bind("tcp://0.0.0.0:%s" % port)
    node.bind("ipc://%s" % SRkvLocalSock)
    server = Greenlet(node.run)

    def exit_proc(*args, **kwargs):
        if os.getpid() == my_pid:
            logger.warning("server exit")
            node.close()
        return

    g_signal(signal.SIGINT, exit_proc)
    logger.info("SRkv server start")
    server.start()
    wait([server])
