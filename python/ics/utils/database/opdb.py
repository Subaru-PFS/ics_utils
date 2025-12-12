import warnings

from typing_extensions import deprecated

from ics.utils.database.db import DB


# Module-level deprecation notice for this convenience subclass
warnings.warn(
    "ics.utils.database.opdb.OpDB is deprecated; please use pfs.utils.database.db.OpDB instead.",
    DeprecationWarning,
    stacklevel=2,
)

@deprecated("ics.utils.database.opdb.OpDB is deprecated; please use pfs.utils.database.db.OpDB instead.")
class OpDB(DB):
    """Deprecated: use pfs.utils.database.db.OpDB instead.

    Operational database convenience subclass of DB.

    Provides sensible defaults for the PFS operational database and otherwise
    behaves like the generic DB helper. You can override any parameter or pass
    a DSN string/mapping; DSN takes precedence as in the base class.

    Examples:
        # Use operational defaults (host=db-ics, user=pfs, dbname=opdb, port=5432)
        db = OpDB()

        # Override specific parameters
        db = OpDB(host="localhost")

        # Or use a DSN string
        db = OpDB("dbname=opdb user=pfs host=db-ics")

    See Also
    --------
    ics.utils.database.db.DB
        The base class that implements connection and query helpers.
    """

    # Default parameters for the operational database
    host = "db-ics"
    user = "pfs"
    dbname = "opdb"
    port = 5432

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "ics.utils.database.opdb.OpDB is deprecated; please use pfs.utils.database.db.OpDB instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
