import warnings

from typing_extensions import deprecated

from ics.utils.database.db import DB


# Module-level deprecation notice for this convenience subclass
warnings.warn(
    "ics.utils.database.qadb.QaDB is deprecated; please use pfs.utils.database.db.QaDB instead.",
    DeprecationWarning,
    stacklevel=2,
)

@deprecated("ics.utils.database.qadb.QaDB is deprecated; please use pfs.utils.database.db.QaDB instead.")
class QaDB(DB):
    """Deprecated: use pfs.utils.database.db.QaDB instead.

    Quality Assurance database convenience subclass of DB.

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

    def __init__(self, *args, **kwargs):
        # Runtime deprecation warning so instantiators get a clear message.
        warnings.warn(
            "ics.utils.database.qadb.QaDB is deprecated; please use pfs.utils.database.db.QaDB instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
