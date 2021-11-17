from datetime import datetime, timedelta

from ics.utils.sps.spectroIds import getSite


def hstTime():
    """Crude HST Time"""
    return datetime.utcnow() - timedelta(hours=10)


class TimeGetter(object):
    """ generate correct datetime according to site"""

    def __init__(self):
        site = getSite()

        if site == 'S':
            self.timedelta = timedelta(hours=10)
            self.timezone = 'HST'
        else:
            self.timedelta = timedelta(hours=0)
            self.timezone = 'UTC'

        self.site = site

    def __call__(self, *args, **kwargs):
        return datetime.utcnow()

    def format(self, timestamp):
        timestamp = timestamp - self.timedelta
        return f'{timestamp.replace(microsecond=0).isoformat()}.{self.timezone}'
