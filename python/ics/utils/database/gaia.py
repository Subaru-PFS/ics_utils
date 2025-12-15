from ics.utils.database.db import ReadOnlyDB


class GaiaDB(ReadOnlyDB):
    """Gaia catalog database convenience subclass of DB.

    Provides sensible defaults for the Gen2 Gaia star catalog database and otherwise
    behaves like the generic DB helper. You can override any parameter or pass
    a DSN string/mapping; DSN takes precedence as in the base class.

    Note: The default connection parameters are for the Hilo base instance.

    Examples:
        # Use operational defaults (host=g2sim-cat, user=obsuser, dbname=star_catalog, port=5438)
        db = GaiaDB()

        # Override specific parameters
        db = GaiaDB(host="localhost")

        # Or use a DSN string
        db = GaiaDB("dbname=g2sim-cat user=obsuser host=g2sim-cat")

    See Also
    --------
    ics.utils.database.db.DB
        The base class that implements connection and query helpers.
    """

    # Default host used for the Gen2 gaia database.
    host = "g2sim-cat"
    user = "obsuser"
    dbname = "star_catalog"
    port = 5438
