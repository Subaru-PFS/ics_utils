import pathlib

import astropy_iers_data as iersData
from astropy.utils.iers import conf as iersConf

"""Set up astropy to use a local version of the IERS files. Whatever
files we need get updated *externally*: we never read from the
network.

There is an official `astropy_iers_data` conda package which someone
keeps updated. If we cron up a `conda update astropy_iers_data` once a
week or so I think we can defer the maintenance and shuffling to
conda-forge and get a single shared data cache (i.e. in our
`/software` for all ICS users.

You would like to think that keeping that package up-to-date would be
all you need to do. Actually, even with that package installed and
wired in (i.e. with IERS_A_FILE pointing to a shiny new file inside
the package), the default `IERS_Auto` will still use the IERS_A_URL to
auto_download into `~/.astropy/cache`. So we need to tell the
downloader to use the package files, but that seems to be all we must
do.

As an aside, casual reading of the astropy IERS docs suggests that you
can set `iers.conf.auto_download = False` and you will be safe from
network delays. True, but astropy will then only use the IERS_B data,
and complain about any resolution after the end of that file, which is
usually a few weeks in the past. Nuts. Might even be a bug.

"""

# Tell the ics_utils clients to download the locally but externally
# updated files whenever astropy feels the need to do so. I.e. set our
# URLs to point to local "file://"s
#
# Once every few hundred times, the downloader fails over to the
# mirror, even when the local files are not changing. Setting the
# mirror to point to the same file seems to paper that over.
#

iersPath = pathlib.Path(iersData.IERS_A_FILE).as_uri()
iersConf.iers_auto_url = iersPath
iersConf.iers_auto_url_mirror = iersPath

iersPath = pathlib.Path(iersData.IERS_LEAP_SECOND_FILE).as_uri()
iersConf.iers_leap_second_auto_url = iersPath
iersConf.iers_leap_second_auto_url_mirror = iersPath

# The conf systen does not seem to cover the IERS_B files, which
# worries me a bit. But the iersData.IERS_B_FILE should be correct and
# up-to-date.
#


