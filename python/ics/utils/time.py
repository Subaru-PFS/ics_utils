from datetime import datetime as pydt

from ics.utils.sps.spectroIds import getSite
from pytz import timezone


class PfsDatetime(pydt):
    """ Regular python datetime with tzformat method to get local timezone isoformat."""

    def setLocalTZ(self, timezone):
        """ Set a local timezone for string conversion. """
        self.localTZ = timezone

    def tzformat(self, fmt='%Y-%m-%d %H:%M:%S %Z'):
        """ Get datetime as isoformat + timezone, no microsecond by default."""
        return self.astimezone(self.localTZ).strftime(fmt)


class datetime(object):
    """ generate correct datetime according to site"""

    UTC = timezone('UTC')
    HST = timezone('US/Hawaii')
    site = getSite()
    localTZ = HST if site == 'S' else UTC

    @classmethod
    def now(cls):
        datetimeObj = pydt.utcnow()
        # force utc and localize this datetime.
        pfsDatetime = PfsDatetime(year=datetimeObj.year, month=datetimeObj.month, day=datetimeObj.day,
                                  hour=datetimeObj.hour, minute=datetimeObj.minute, microsecond=datetimeObj.microsecond,
                                  tzinfo=datetime.UTC)
        # set local timezone.
        pfsDatetime.setLocalTZ(cls.localTZ)
        return pfsDatetime
