import pathlib
import warnings

from astropy.units import d as day
from astropy.utils import iers

"""Set up astropy to use a local version of the IERS files. Whatever
files we need get updated *externally*: we never read from the
network.

We want a merged version of IERS_B, which provides measured
*historical* data, and IERS_A, which if recent provides some
predictions. The `astropy.utils.iers` package has routines which read
the two sources and merges them, but `astropy` is not configured to do
that by default.

There is an official `astropy_iers_data` conda package which someone
keeps updated. If we cron up a `conda update astropy_iers_data` once a
week or so I think we can defer the maintenance and shuffling to
conda-forge and get a single shared data cache (i.e. in our
`/software` for all ICS users.

You would like to think that keeping that package up-to-date would be
all you need to do. Actually, even with that package installed and
wired in (i.e. with IERS_A_FILE pointing to a shiny new file inside
the package), the default `IERS_Auto` will still auto_download into
`~/.astropy/cache`. Nuts.

Further casual reading of the astropy IERS docs suggests that you can
set `iers.conf.auto_download = False` and you will be safe from
network delays. True, but astropy will then only use the IERS_B data,
and complain about any resolution after the end of that file, which is
usually a few weeks in the past. Nuts. Might even be a bug.

I believe that instead you need to directly load the merged table and
tell the IERS machinery to use that. Easy enough to do, but hard to
discover.
"""

# Make sure it doesn't wake up and try to be smart.
iers.conf.auto_download = False

# The IERS_A.read() routine is what reads IERS_B and IERS_A, and
# merges them properly.
iers.earth_orientation_table.set(iers.IERS_A.read())

# Tests:
# - make sure we point at the astropy_iers_data files
iersPath = pathlib.Path(iers.IERS_A_FILE)
if not iersPath.match('*/astropy_iers_data/data/finals2000A.all'):
    warnings.warn(f'astropy.iers.IERS_A_FILE does not point into the astropy_iers_data package: {iersPath}')

# - make sure that file is up-to-date.
#   Use starting UT1 prediction date being < 10 days as a proxy.
iersTable = iers.earth_orientation_table.get()
today = iersTable.time_now.mjd * day
firstPrediction = iersTable[iersTable['UT1Flag'] == 'P'][0]['MJD']
ok = (today - firstPrediction) > 10*day
if not ok:
    warnings.warn(f'astropy_iers_data conda module getting stale: consider update ({today - firstPrediction}')
