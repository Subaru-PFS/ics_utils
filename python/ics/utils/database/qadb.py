from ics.utils.database.db import DB


class QaDB(DB):
    """Quality Assurance database convenience subclass of DB.

    Provides sensible defaults for the PFS QA database and otherwise
    behaves like the generic DB helper. You can override any parameter or pass
    a DSN string/mapping; DSN takes precedence as in the base class.

    Examples:
        # Use QADB defaults (host=pfsa-db, user=pfs, dbname=qadb, port=5436)
        db = QaDB()

        # Override specific parameters
        db = QaDB(host="localhost")

        # Or use a DSN string
        db = QaDB("dbname=qadb user=pfs host=pfsa-db")

    See Also
    --------
    ics.utils.database.db.DB
        The base class that implements connection and query helpers.
    """

    # Default parameters for the QA database
    host = "pfsa-db"
    user = "pfs"
    dbname = "qadb"
    port = 5436
