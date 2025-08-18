# ics_utils - ICS utility package

Common utility tools for the Subaru Prime Focus Spectrograph (PFS) Instrument Control System (ICS).

## Overview

- This package provides shared Python utilities used by PFS ICS actors and related services.
- It includes helpers for FITS file writing and metadata, database access, instrument configuration and data access,
  state machines, TCP communication, spectrograph subsystems (lamps and PDU), visit/field bookkeeping, and more.
- The repository also contains Lua scripts for Digital Loggers power devices used by some subsystems.

## Features

- Database utilities
    - Connection classes for the databases used by ICS, including:
        - OpDB (Operational database)
        - GaiaDB (GAIA star catalog database)
        - QaDB (Quality Assurance database)
- FITS utilities
    - Structured FITS writer for PFS-specific headers and data (ics.utils.fits)
    - WCS/timecards helpers and convenience functions
- Finite State Machine (FSM)
    - Lightweight FSM framework used by actors/threads (ics.utils.fsm)
- Instrument data/config
    - Access to actor/instrument configuration and persistent data (ics.utils.instdata)
- TCP utilities
    - Buffered sockets and convenience methods (ics.utils.tcp)
- Spectrograph (SPS) helpers
    - Lamps and PDU abstractions, controllers (including Digital Loggers/Aten), simulators, and command helpers (
      ics.utils.sps)
- Visit management
    - Visit/field data structures and manager utilities (ics.utils.visit)
- Misc utilities
    - Time, threading helpers, MHS integration, logbook support, versions helper

## Usage

### Database usage

- Authentication: Passwords are expected to be managed externally by libpq (e.g., via ~/.pgpass). The helpers use
  psycopg2 through SQLAlchemy and do not embed passwords.

#### Operational DB convenience class

```python
from ics.utils.database.opdb import OpDB

# Uses default connection settings for the PFS operational DB
opdb = OpDB()
rows = opdb.fetchone("SELECT max(pfs_visit_id) FROM pfs_visit")
```

##### Legacy API (deprecated)

Note: Deprecated legacy module `ics.utils.opdb` in favor of the class-based database API at
`ics.utils.database.opdb.opDB`.

###### Migration guidance

- Previous usage (deprecated):
    - `from ics.utils import opdb` or `from ics.utils.opdb import opDB` and then static calls like `opDB.fetchall(...)`.
- Recommended usage:
    - `from ics.utils.database.opdb import OpDB`
    - Create an instance and use instance methods for connections and queries:
        - `db = OpDB()`  # defaults to dbname=opdb, user=pfs, host=pfsa-db
        - `rows = db.fetchall("SELECT ...", params)`
        - `row = db.fetchone("SELECT ...", params)`
        - `db.insert("table", column=value, ...)`

```python
from ics.utils.opdb import opDB

# Warning: this legacy API is deprecated; prefer ics.utils.database.opdb.OpDB
# It returns a raw psycopg2 connection when using connect()
with opDB.connect() as psyco_conn:
    with psyco_conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchall())

# Convenience wrappers mirroring the new API
rows = opDB.fetchall("SELECT 1")
```

#### Gaia catalog database (read-only)

```python
from ics.utils.database.gaia import GaiaDB

gaia = GaiaDB()  # defaults: host=g2sim-cat, user=obsuser, dbname=star_catalog, port=5438
# Read queries are allowed
stars = gaia.fetchall("SELECT ra, dec, phot_g_mean_mag FROM gaia3 LIMIT 5")

# Writes are intentionally no-ops and will emit a warning.
gaia.insert("gaia3", ra=0)  # no-op
```

#### Generic DB usage

```python
from ics.utils.database.db import DB

# Option 1: provide parameters
opdb = DB(dbname="opdb", user="pfs", host="db-ics", port=5432)
rows = opdb.fetchall("SELECT 1 AS one")
print(rows)  # numpy array of rows

# Option 2: DSN string
opdb2 = DB("dbname=opdb user=pfs host=db-ics port=5432")

# Option 3: mapping
opdb3 = DB({"dbname": "opdb", "user": "pfs", "host": "db-ics", "port": 5432})

# Fetch one row with parameters
n = opdb.fetchone("SELECT %(x)s::int AS val", {"x": 42})

# Insert helper (builds an INSERT ... VALUES ... using named parameters)
opdb.insert(
    "pfs_visit",
    pfs_visit_id=1,
    pfs_visit_description="i am the first pfs visit",
)

# Reuse one connection for multiple statements
with opdb.connection() as conn:
    conn.exec_driver_sql("SET LOCAL statement_timeout = 5000")
    conn.exec_driver_sql("SELECT 1")
```

Notes

- Connection pooling: DB/OpDB cache a SQLAlchemy Engine with pooling. Each helper method checks out a connection for the
  duration of the call. Use db.connection() to explicitly reuse a single connection.
- Result format: fetchone/fetchall return numpy arrays for backward compatibility.

## Installation

### Requirements

- Python: >= 3.11

### EUPS Installation with LSST Stack

This package uses the Extended Unix Product System (EUPS) for dependency management and environment setup, which is part
of the LSST Science Pipelines software stack. The LSST stack is a comprehensive framework for astronomical data
processing that provides powerful tools for image processing, astrometry, and data management.

1. Ensure you have the LSST stack installed on your system. If not, follow the installation instructions at
   the [LSST Science Pipelines documentation](https://pipelines.lsst.io/install/index.html).

2. Once the LSST stack is set up, declare and setup this package using EUPS:
   ```
   eups declare -r /path/to/ics_utils ics_utils git
   setup -r /path/to/ics_utils
   ```

### Standard Setup

Alternatively, you can install the package using pip:

1. Clone the repository:
   ```
   git clone https://github.com/Subaru-PFS/ics_utils.git
   ```

2. Install the package:
   ```
   cd ics_utils
   pip install .
   ```

## Project Structure

- python/ics/utils/
    - fits/: FITS writing helpers
    - fsm/: FSM utilities
    - instdata/: instrument/actor configuration and IO
    - sps/: spectrograph related helpers (lamps, PDU, parts, spectra IDs)
    - tcp/: TCP socket helpers
    - visit/: visit and field models and manager
    - time.py, threading.py, opdb.py, logbook.py, versions.py, etc.
- lua/digitalLoggers/: Lua scripts for Digital Loggers devices

## License

This project is part of the Subaru Prime Focus Spectrograph (PFS) project and is subject to the licensing terms of the
PFS collaboration.

## Contact

For questions or issues related to this software, please contact the PFS software team or create an issue in the
repository.
