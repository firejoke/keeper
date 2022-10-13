# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2022/5/12 16:41
from sqlalchemy import (
    Boolean, Column, DateTime, Integer, PickleType, String, Table, func
)
from sqlalchemy.orm import mapper

from public_def import ModelBase


_format = '%Y-%m-%d %H:%M:%f'


repository = Table(
    "repository", ModelBase.metadata,
    Column("key", String(256), primary_key=True, unique=True, nullable=False),
    Column("value", PickleType),
    Column(
        "create_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
    ),
    Column(
        "update_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
        onupdate=func.strftime(_format, 'now', 'localtime', type_=DateTime)
    ),
)


class Repository(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value


mapper(Repository, repository)


transaction = Table(
    "srkv_transaction", ModelBase.metadata,
    Column("id", Integer, primary_key=True),
    Column("action", String(128), nullable=False),
    Column("key", String(128), nullable=False),
    Column("value", PickleType, nullable=True),
    Column("roll_action", String(128), nullable=False),
    Column("roll_value", PickleType, nullable=True),
    Column("state", String(16), default="ready"),
    Column("uuid", String(16), unique=True, nullable=False),
    Column(
        "create_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
    ),
    Column(
        "update_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
        onupdate=func.strftime(_format, 'now', 'localtime', type_=DateTime)
    ),
)


class Transaction(object):
    def __init__(
        self, id, uuid, action, key, value, roll_action, roll_value, state
    ):
        self.id = id
        self.uuid = uuid
        self.action = action
        self.key = key
        self.value = value
        self.roll_action = roll_action
        self.roll_value = roll_value
        self.state = state


mapper(Transaction, transaction)


class Nodes(ModelBase):
    __tablename__ = "nodes"
    sys_uid = Column(String(512), primary_key=True)
    ipaddresses = Column(String(255), nullable=False)
