import warnings
from contextlib import contextmanager
from typing import Any, Mapping, Optional, Union

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine


class DB:
    """Generic DB helper that accepts a DSN string or connection parameters.

    This class caches a SQLAlchemy Engine, which manages a pool of connections.
    Individual methods check out a connection from the pool for the duration of
    the call and then return it to the pool, ensuring proper connection reuse.
    For explicit reuse of a single connection across multiple operations, use
    the public `connection()` context manager.

    Note:
        Password is expected to be managed externally (e.g., ~/.pgpass).

    Usage examples:
        # Using keyword parameters
        db = DB(dbname='opdb', user='pfs', host='db-ics')

        # Using a DSN string
        db = DB('dbname=opdb user=pfs host=db-ics')

        # Using a mapping/dict
        db = DB({'dbname': 'opdb', 'user': 'pfs', 'host': 'db-ics'})
    """

    host = "localhost"
    user = "user"
    dbname = "dbname"
    port = 5432

    def __init__(
        self,
        dsn: Optional[Union[str, Mapping[str, Any]]] = None,
        host: str | None = None,
        user: str | None = None,
        dbname: str | None = None,
        port: int | None = None,
    ) -> None:
        """Initialize a DB instance.
        Parameters
        ----------
        host : str, optional
            Database host name.
        user : str, optional
            Database user name.
        dbname : str, optional
            Database name.
        port : int, optional
            Database port number, default is 5432.
        dsn : str or Mapping[str, Any], optional
            A DSN string (e.g., "dbname=opdb user=pfs host=db-ics") or a mapping
            of connection parameters. If provided, it takes precedence over host/user/dbname.
        """
        self.host = host if host is not None else self.host
        self.user = user if user is not None else self.user
        self.dbname = dbname if dbname is not None else self.dbname
        self.port = port if port is not None else self.port

        self._dsn: Optional[Union[str, Mapping[str, Any]]] = None
        self._engine: Optional[Engine] = None
        if dsn is not None:
            self.dsn = dsn

    @property
    def dsn(self) -> str:
        """The DSN string or mapping used for the connection, if any."""
        if self._dsn is None:
            self._dsn = f"dbname={self.dbname} user={self.user} host={self.host} port={self.port}"

        return self._dsn  # type: ignore[return-value]

    @dsn.setter
    def dsn(self, value: Union[str, Mapping[str, Any]]) -> None:
        """Set the DSN string or mapping used for the connection."""
        self._dsn = value
        # Parse a libpq-style DSN string to update attributes
        if isinstance(value, str):
            for k, v in (item.split("=") for item in value.split() if "=" in item):
                if k == "host":
                    self.host = v
                elif k == "user":
                    self.user = v
                elif k == "dbname":
                    self.dbname = v
                elif k == "port":
                    self.port = int(v)
        elif isinstance(value, Mapping):
            if "host" in value:
                self.host = value["host"]
            if "user" in value:
                self.user = value["user"]
            if "dbname" in value:
                self.dbname = value["dbname"]
            if "port" in value:
                self.port = int(value["port"])  # type: ignore[arg-type]
        # Reset engine so it can be re-created with new params
        self._engine = None

    def _build_url(self) -> str:
        """Build a SQLAlchemy URL for PostgresSQL using current attributes.

        We intentionally omit the password; libpq will use ~/.pgpass if available.
        """
        user = self.user or ""
        host = self.host or ""
        dbname = self.dbname or ""
        port = f":{self.port}" if self.port is not None else ""
        # Use psycopg2 driver via SQLAlchemy.
        return f"postgresql+psycopg2://{user}@{host}{port}/{dbname}"

    @property
    def engine(self) -> Engine:
        """Create or return a cached SQLAlchemy Engine."""
        if self._engine is None:
            # If a mapping DSN was provided, prefer those values
            url = self._build_url()
            self._engine = create_engine(url, pool_pre_ping=True, future=True)
        return self._engine

    def connect(self) -> Connection:
        """Return a new SQLAlchemy connection using provided DSN/params.

        Notes:
            Connections are provided by SQLAlchemy's connection pool managed by the
            cached Engine. Each call checks out a connection from the pool and, when
            closed, returns it to the pool for reuse.
        """
        return self.engine.connect()

    @contextmanager
    def connection(self):
        """Public context manager yielding a pooled SQLAlchemy connection.

        Use this to explicitly reuse the same connection across multiple statements:

            with db.connection() as conn:
                conn.exec_driver_sql("SELECT 1")
                conn.exec_driver_sql("SELECT 2")

        This reduces per-call checkout/return overhead while still leveraging
        SQLAlchemy's pooling. The underlying cursor lifecycle is managed by the
        DBAPI driver (psycopg2) per execution.
        """
        with self.connect() as conn:
            yield conn

    def fetchall(self, query: str, params: Optional[Union[tuple, list, dict]] = None):
        """Fetch all rows for a query as a numpy array.

        Uses exec_driver_sql to preserve native paramstyles like %(name)s or %s.
        """
        with self.connection() as conn:
            result = conn.exec_driver_sql(query, params)  # type: ignore[arg-type]
            rows = result.fetchall()
            return np.array(rows)

    def fetchone(self, query: str, params: Optional[Union[tuple, list, dict]] = None):
        """Fetch a single row for a query as a numpy array.

        Uses exec_driver_sql to preserve native paramstyles like %(name)s or %s.
        """
        with self.connection() as conn:
            result = conn.exec_driver_sql(query, params)  # type: ignore[arg-type]
            row = result.fetchone()
            return np.array(row)

    def commit(self, query: str, params: Optional[Union[tuple, list, dict]] = None):
        """Execute a query within a transaction and commit it."""
        with self.engine.begin() as conn:
            conn.exec_driver_sql(query, params)  # type: ignore[arg-type]

    def insert(self, table: str, **kwargs: Any):
        """Insert a row into a table using kwargs as column/value pairs.
        Args:
            table (str): table name
        Examples:
            >>> DB(dbname='opdb', user='pfs', host='db-ics').insert(
            ...     'pfs_visit', pfs_visit_id=1, pfs_visit_description='i am the first pfs visit')
        """
        fields = ", ".join(kwargs.keys())
        # Keep psycopg2-style named parameters to remain backward compatible
        values = ", ".join(["%(" + v + ")s" for v in kwargs])
        query = f"INSERT INTO {table} ({fields}) VALUES ({values})"
        return self.commit(query, kwargs)


class ReadOnlyDB(DB):
    """Read-only database convenience subclass of DB.

    This class merely overrides the `commit` and `insert` methods.

    """

    def commit(self, query: str, params: Optional[Union[tuple, list, dict]] = None):
        """No-op commit for read-only Gaia database.

        This method intentionally does nothing to prevent write operations
        on the read-only Gaia catalog database.
        """
        warnings.warn("commit() is a no-op for the read-only Gaia database", UserWarning)
        return None

    def insert(self, table: str, **kwargs: Any):
        """No-op insert for read-only Gaia database.

        This method intentionally does nothing to prevent write operations
        on the read-only Gaia catalog database.
        """
        warnings.warn("insert() is a no-op for the read-only Gaia database", UserWarning)
        return None
