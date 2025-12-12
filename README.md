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

### Database usage (DEPRECATED)

This package's database helpers (for example, `ics.utils.database.db.DB`,
`ics.utils.database.opdb.OpDB`, `ics.utils.database.gaia.GaiaDB`, and
`ics.utils.database.qadb.QaDB`) are deprecated. The implementations and
canonical documentation have moved to the separate `pfs_utils` project.

Please consult the `pfs_utils` README and use the `pfs.utils.database` module
there (for example, `pfs.utils.database.db.DB`) instead of the classes from
this repository. The `ics_utils` copies remain only for backward compatibility
and will emit deprecation warnings at runtime; they may be removed in a future
release.

For migration:

- Replace imports like `from ics.utils.database.db import DB` with
  `from pfs.utils.database.db import DB`.
- Check the documentation at `pfs_utils` for any changes in usage or API.

If you need a direct pointer to the upstream documentation, see the README in
the `pfs_utils` project (the authoritative docs for the database helpers).

### Other usage sections remain unchanged.

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
