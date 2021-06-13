from __future__ import annotations

import logging
import re
import uuid
from abc import ABC, abstractmethod
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Optional, Callable, Union, Any, Dict

import psycopg2
import psycopg2.extras as extras

logger = logging.getLogger(__name__)


def _serialize(dict_: Mapping[str, Any]):
    return {key: str(value) if isinstance(value, uuid.UUID) else value for key, value in dict_.items()}


def insert(cursor, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
    if not values:
        return
    if isinstance(values, Mapping):
        values = [values]
    keys = values[0].keys()
    insertion_points = ','.join([f'"{key}"' for key in keys])
    insertion_pattern = '(' + ','.join(f"%({key})s" for key in keys) + ')'
    values = ','.join(
        cursor.mogrify(insertion_pattern, _serialize(dict_)).decode('utf8') for dict_ in values)
    # noinspection SqlResolve,SqlNoDataSourceInspection
    query = f'INSERT INTO "{table}" ({insertion_points}) VALUES {values}'
    logger.debug(f"Executing query: {query}")
    cursor.execute(query)


class Select:
    def __init__(self, parent: Union[Database, Session, Transaction], query: str, *args, **kwargs):
        self._query: str = query
        if args:
            self._args = args
        elif kwargs:
            self._args = kwargs
        else:
            self._args = None
        self._parent = parent
        self._session = None
        self._transaction = None
        self._cursor = None

    def __iter__(self):
        logger.debug(f"Executing query {self._query} with args: {self._args}")
        self._cursor.execute(self._query, self._args)
        return self

    def __next__(self):
        res = self._cursor.fetchone()
        if res is None:
            raise StopIteration
        return res

    def __enter__(self):
        if isinstance(self._parent, Database):
            self._session = self._parent.__enter__()
            self._transaction = self._session.__enter__()
            self._cursor = self._transaction.cursor()
        elif isinstance(self._parent, Session):
            self._transaction = self._parent.__enter__()
            self._cursor = self._transaction.cursor()
        elif isinstance(self._parent, Transaction):
            self._cursor = self._parent.cursor()
        self._cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._cursor.__exit__(exc_type, exc_val, exc_tb)
        finally:
            try:
                if isinstance(self._parent, Database):
                    try:
                        self._transaction.__exit__(exc_type, exc_val, exc_tb)
                    finally:
                        self._session.__exit__(exc_type, exc_val, exc_tb)
                elif isinstance(self._parent, Session):
                    self._parent.__exit__(exc_type, exc_val, exc_tb)
            finally:
                self._session = None
                self._transaction = None
                self._cursor = None


class SelectMock(Select):
    def __init__(self, data: Collection[Collection]):
        # noinspection PyTypeChecker
        super().__init__(None, "")
        self._iter = iter(data)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Queryable(ABC):
    @abstractmethod
    def select(self, query, *args, **kwargs) -> Select:
        raise NotImplementedError

    @abstractmethod
    def insert(self, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        raise NotImplementedError

    @abstractmethod
    def execute(self, sql: str, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def mock(fakes: Dict[str, Collection[Collection]] = dict()) -> Queryable:
        return QueryableMock(fakes)


class QueryableMock(Queryable):
    def __init__(self, fakes: Dict[str, Collection[Collection]]):
        self._fakes = fakes

    def select(self, query, *args, **kwargs) -> Select:
        for k, v in self._fakes.items():
            if re.match(k, query):
                return SelectMock(v)
        return SelectMock([])

    def insert(self, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        # don't do anything
        pass

    def execute(self, sql: str, *args, **kwargs):
        # don't do anything
        pass


@dataclass
class Credentials:
    username: str
    password: str
    database: str
    hostname: str = 'localhost'
    port: str = 5432

    def __str__(self):
        return f"dbname={self.database}" \
               f" user={self.username}" \
               f" password={self.password}" \
               f" host={self.hostname}" \
               f" port={self.port}"


class Database(Queryable):
    def select(self, query, *args, **kwargs) -> Select:
        return Select(self, query, *args, **kwargs)

    def insert(self, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        with self as session:
            session.insert(table, values)

    def execute(self, sql: str, *args, **kwargs):
        with self as session:
            session.execute(sql, *args, **kwargs)

    def __init__(self,
                 /,
                 arg=None,
                 *,
                 credentials: Optional[Credentials] = None,
                 connection_string: Optional[str] = None,
                 connection_factory: Optional[Callable] = None):
        if isinstance(arg, Credentials):
            credentials = arg
        elif isinstance(arg, str):
            connection_string = arg
        elif isinstance(arg, Callable):
            connection_factory = arg
        if credentials is not None:
            connection_string = str(credentials)
        if connection_string is not None:
            connection_factory = lambda: psycopg2.connect(connection_string)
        if connection_factory is None:
            raise ValueError("no credentials, connection_string, or connection_factory given")
        self._connection_factory = connection_factory

    def __enter__(self) -> Session:
        conn = self._connection_factory()
        if isinstance(conn, str):
            conn = psycopg2.connect(conn)
        self._session = Session(conn)
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # noinspection PyProtectedMember
            self._session._conn.close()
        finally:
            self._session = None

    @staticmethod
    def mock(fakes: Dict[str, Collection[Collection]] = dict()) -> Database:
        # noinspection PyTypeChecker
        return QueryableMock.mock(fakes)


class GlobalSession(Queryable):
    __database = None
    __session = None
    __args = []
    __kwargs = {}

    def __init__(self, *args, **kwargs):
        if args or kwargs:
            if GlobalSession.__database is not None:
                GlobalSession.__database.__exit__(None, None, None)
                GlobalSession.__session = None
            GlobalSession.__args = args
            GlobalSession.__kwargs = kwargs

    @classmethod
    def get(cls) -> Session:
        if cls.__database is None:
            cls.__database = Database(*cls.__args, **cls.__kwargs)
            cls.__session = cls.__database.__enter__()
        return cls.__session

    @classmethod
    def select(cls, query, *args, **kwargs) -> Select:
        return Select(cls.get(), query, *args, **kwargs)

    @classmethod
    def insert(cls, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        with cls.get() as transaction:  # pylint: disable=E1129
            with transaction.cursor() as cur:
                insert(cur, table, values)

    @classmethod
    def execute(cls, sql: str, *args, **kwargs):
        with cls.get() as transaction:  # pylint: disable=E1129
            transaction.execute(sql, *args, **kwargs)


class Session(Queryable):
    def select(self, query, *args, **kwargs) -> Select:
        return Select(self, query, *args, **kwargs)

    def insert(self, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        with self as transaction:
            with transaction.cursor() as cur:
                insert(cur, table, values)

    def execute(self, sql: str, *args, **kwargs):
        with self as transaction:
            transaction.execute(sql, *args, **kwargs)

    def __init__(self, conn):
        self._conn = conn
        self._transaction = None

    def __enter__(self) -> Transaction:
        assert self._transaction is None
        self._conn.__enter__()
        self._transaction = Transaction(self._conn)
        return self._transaction

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._transaction is not None
        try:
            self._conn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._transaction = None

    @staticmethod
    def mock(fakes: Dict[str, Collection[Collection]] = dict()) -> Session:
        # noinspection PyTypeChecker
        return QueryableMock.mock(fakes)


class Transaction(Queryable):
    def cursor(self):
        return self._conn.cursor()

    def select(self, query, *args, **kwargs) -> Select:
        return Select(self, query, *args, **kwargs)

    def insert(self, table: str, values: Union[Collection[Mapping[str, Any]], Mapping[str, Any]]):
        with self._conn.cursor() as cur:
            insert(cur, table, values)

    def execute(self, sql: str, *args, **kwargs):
        with self as cur:
            cur.execute(sql, *args, **kwargs)

    def rollback(self):
        self._conn.rollback()

    def commit(self):
        self._conn.commit()

    def __init__(self, conn):
        self._conn = conn
        self._cursor = None

    def __enter__(self):
        assert self._cursor is None
        cur = self._conn.cursor()
        self._cursor = Cursor(cur)
        # noinspection PyProtectedMember
        self._cursor._cur.__enter__()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._cursor is not None
        try:
            # noinspection PyProtectedMember
            self._cursor._cur.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._cursor = None


class Cursor:
    def execute(self, sql: str, *args, **kwargs):
        self._cur.execute(sql, *args, **kwargs)

    def execute_batch(self, sql: str, *args, **kwargs):
        extras.execute_batch(self._cur, sql, *args, **kwargs)

    def __init__(self, cur):
        self._cur = cur
