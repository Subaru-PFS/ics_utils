from ics.utils.database.db import DB


class OpDB(DB):
    """Operational database convenience subclass of DB.

    Provides sensible defaults for the PFS operational database and otherwise
    behaves like the generic DB helper. You can override any parameter or pass
    a DSN string/mapping; DSN takes precedence as in the base class.

    Examples:
        # Use operational defaults (host=pfsa-db, user=pfs, dbname=opdb, port=5432)
        db = OpDB()

        # Override specific parameters
        db = OpDB(host="localhost")

        # Or use a DSN string
        db = OpDB("dbname=opdb user=pfs host=pfsa-db")

    See Also
    --------
    ics.utils.database.db.DB
        The base class that implements connection and query helpers.
    """

    # Default parameters for the operational database
    host = "pfsa-db"
    user = "pfs"
    dbname = "opdb"
    port = 5432
