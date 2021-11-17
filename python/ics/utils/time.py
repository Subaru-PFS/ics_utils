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
            self.getTime = hstTime
            self.timezone = 'HST'
        else:
            self.getTime = datetime.utcnow
            self.timezone = 'UTC'

        self.site = site

    def __call__(self, *args, **kwargs):
        return self.getTime()

    def format(self, timestamp):
        return f'{timestamp.replace(microsecond=0).isoformat()}.{self.timezone}'
