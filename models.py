# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2022/5/12 16:41
from sqlalchemy import (
    Boolean, Column, DateTime, Integer, PickleType, String, Table, func, text,
)
from sqlalchemy.orm import mapper

from public_def import ModelBase


_format = '%Y-%m-%d %H:%M:%f'


repository = Table(
    "repository", ModelBase.metadata,
    Column("id", Integer, primary_key=True),
    Column("key", String(256), unique=True, index=True),
    Column("value", PickleType),
    Column("deleted", Boolean),
    Column(
        "create_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
    ),
    Column(
        "update_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
        onupdate=func.strftime(_format, 'now', 'localtime', type_=DateTime)
    ),
    sqlite_autoincrement=True
)


class Repository(object):
    def __init__(self, key, value, deleted):
        self.key = key
        self.value = value
        self.deleted = deleted


mapper(Repository, repository)


transaction = Table(
    "transaction", ModelBase.metadata,
    Column("id", Integer, primary_key=True),
    Column("action", String(128)),
    Column("key", String(128)),
    Column("value", PickleType),
    Column("roll_action", String(128), nullable=True),
    Column("roll_value", PickleType, nullable=True),
    Column("state", String(16), default="ready"),
    Column(
        "create_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
    ),
    Column(
        "update_time", DateTime,
        server_default=func.strftime(_format, 'now', 'localtime', type_=DateTime),
        onupdate=func.strftime(_format, 'now', 'localtime', type_=DateTime)
    ),
    # sqlite_autoincrement=True
)


class Transaction(object):
    def __init__(self, id, action, key, value, roll_action, roll_value, state):
        self.id = id
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
    ipaddresses = Column(String(255))
