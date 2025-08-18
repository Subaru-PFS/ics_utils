import warnings

import psycopg2

from ics.utils.database.opdb import OpDB

# Create a single OpDB instance to back the legacy API
_opdb_instance = OpDB()


class opDB:
    """Legacy static helper for the operational database.

    Backed by an instance of ics.utils.database.opdb.OpDB for all operations
    to keep behavior consistent with the new database layer while preserving
    the legacy static method API.
    """

    # Keep legacy attribute; callers may override this before invoking methods.
    host = "db-ics"

    @staticmethod
    def _sync_instance():
        """Sync legacy class attributes to the underlying OpDB instance."""
        _opdb_instance.host = opDB.host
        # Keep user/dbname/port from the OpDB defaults unless changed elsewhere.
        return _opdb_instance

    @staticmethod
    def connect():
        """Return a raw psycopg2 connection for backward compatibility.

        Password is expected to be provided via ~/.pgpass as before.
        """
        warnings.warn(
            "ics.utils.opdb.opDB is deprecated and will be removed in a future release. "
            "Use ics.utils.database.opdb.OpDB instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        inst = opDB._sync_instance()
        return psycopg2.connect(dbname=inst.dbname, user=inst.user, host=inst.host, port=inst.port)

    @staticmethod
    def fetchall(query, params=None):
        """Fetch all rows from query as a numpy array (legacy behavior)."""
        inst = opDB._sync_instance()
        return inst.fetchall(query, params)

    @staticmethod
    def fetchone(query, params=None):
        """Fetch one row from query as a numpy array (legacy behavior)."""
        inst = opDB._sync_instance()
        return inst.fetchone(query, params)

    @staticmethod
    def commit(query, params=None):
        """Execute query within a transaction and commit (delegated)."""
        inst = opDB._sync_instance()
        return inst.commit(query, params)

    @staticmethod
    def insert(table, **kwargs):
        """Insert row in table, column names and values are parsed as kwargs.
        Args:
            table (str): table name
        Examples:
            >>> opDB.insert('pfs_visit', pfs_visit_id=1, pfs_visit_description='i am the first pfs visit')
        """
        inst = opDB._sync_instance()
        return inst.insert(table, **kwargs)
