import time
from datetime import datetime

from astropy import time as astroTime
from ics.utils.sps.spectroIds import getSite
from pytz import timezone

UTC = timezone('UTC')
HST = timezone('US/Hawaii')

site = getSite()


def timestamp():
    """Return the time in seconds since the epoch as a floating point number."""
    # mostly useful because it's fast and we can easily convert to PfsTime if necessary.
    return time.time()


class Time(astroTime.core.Time):
    """ generate correct datetime according to site"""
    localTZ = HST if site == 'S' else UTC

    @classmethod
    def fromdatetime(cls, localized):
        """ Convert to PfsTime from localized datetime object.

        Parameters
        ----------
        localized : `datetime`
           localized datetime.
        Returns
        -------
        pfsTime: `Time`
        """
        # make sure datetime is actually localized
        if localized.tzinfo is None:
            raise RuntimeError(f'{localized} is not localized...')

        # convert to UTC in any case.
        utcTime = localized.astimezone(UTC)
        # convert to astroTime.
        return cls(utcTime)

    @staticmethod
    def now():
        """ Return current date and time as pfsTime.
        Returns
        -------
        pfsTime: `Time`
        """
        # force utc and localize this datetime.
        utcTime = datetime.now(UTC)
        # use from datetime constructor
        return Time.fromdatetime(utcTime)

    @staticmethod
    def fromtimestamp(timestamp):
        """ Convert to pfsTime from unix timestamp.

         Parameters
        ----------
        timestamp : `float`
           unix timestamp.
        Returns
        -------
        pfsTime: `Time`
        """
        # convert to localized datetime
        localized = convert.datetime_from_timestamp(timestamp)
        # use from datetime constructor
        return Time.fromdatetime(localized)

    @staticmethod
    def fromisoformat(datestr):
        """ Convert to pfsTime from isoformat datestr.

         Parameters
        ----------
        datestr : `str`
           isoformat datestr.
        Returns
        -------
        pfsTime: `Time`
        """
        # convert to localized datetime
        localized = convert.datetime_from_isoformat(datestr)
        # set local timezone.
        # use from datetime constructor
        return Time.fromdatetime(localized)

    def to_datetime(self, timezone=None):
        """ Convert to datetime.
        Returns
        -------
        datetime: `datetime`
        """
        # ignore timezone, Time is always referenced to UTC.
        return astroTime.core.Time.to_datetime(self, timezone=UTC)

    def timestamp(self):
        """ Convert to unix timestamp.
        Returns
        -------
        timestamp: `float`
        """
        return self.to_datetime().timestamp()

    def isoformat(self, microsecond=True):
        """ Convert to isoformat.
        Returns
        -------
        datestr: `str`
        """
        return convert.datetime_to_isoformat(self.to_datetime(), microsecond=microsecond)


class convert(object):
    @staticmethod
    def datetime_from_isoformat(datestr):
        """ Convert to localized datetime from isoformat.

         Parameters
        ----------
        datestr : `str`
           isoformat datestr.
        Returns
        -------
        localized: `datetime`
        """
        # ugly but could not find any quick workaround.
        iso, tz = (datestr[:-1], UTC) if datestr[-1] == 'Z' else (datestr, HST)
        # convert from iso
        local = datetime.fromisoformat(iso)
        # convert to timezone and localize
        localized = tz.localize(local)
        return localized

    @staticmethod
    def datetime_from_timestamp(timestamp):
        """ Convert to localized datetime from unix timestamp.

         Parameters
        ----------
        timestamp : `float`
           isoformat datestr.
        Returns
        -------
        localized: `datetime`
        """
        # simple conversion, timestamp should always be epoch UTC.
        return datetime.fromtimestamp(timestamp, tz=UTC)

    @staticmethod
    def datetime_to_isoformat(datetime, microsecond=True):
        """ Convert localized datetime to isoformat.

         Parameters
        ----------
        localized : `datetime`
           isoformat datestr.
        Returns
        -------
        pfsTime: `Time`
        """
        # make sure datetime is actually localized
        if datetime.tzinfo is None:
            raise RuntimeError(f'{datetime} is not localized...')

        fmt = '%Y-%m-%dT%H:%M:%S'
        # add microsecond if necessary.
        fmt = f'{fmt}.%f' if microsecond else fmt
        # add Z for UTC and nothing for local, eg HST.
        fmt = f'{fmt}Z' if Time.localTZ == UTC else fmt
        return datetime.astimezone(Time.localTZ).strftime(fmt)


class sleep(object):
    """ generate correct datetime according to site"""

    @staticmethod
    def millisec(value=1):
        time.sleep(value / 1000)
