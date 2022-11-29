__author__ = 'alefur'

import ics.utils.time as pfsTime

warmingTime = dict(neon=15, xenon=15, krypton=15, argon=15, halogen=15, hgar=60, hgcd=60)
allLamps = list(warmingTime.keys())


class LampState(object):
    """ Handle lamp state and keywords. """

    def __init__(self):
        self.state = 'unknown'
        self.onTimestamp = self.offTimestamp = pfsTime.timestamp()

    @property
    def lampOn(self):
        return self.state == 'on'

    def __str__(self):
        return ','.join([f'{self.state}',
                         f'{pfsTime.Time.fromtimestamp(self.offTimestamp).isoformat()}',
                         f'{pfsTime.Time.fromtimestamp(self.onTimestamp).isoformat()}'])

    def setState(self, state, genTimeStamp=False):
        """ Update current state and generate timestamp is requested. """
        self.state = state
        if genTimeStamp:
            self.genTimeStamp()

    def genTimeStamp(self):
        """ Generate timestamp as utc time. """
        now = pfsTime.timestamp()

        if self.lampOn:
            self.onTimestamp = now
        else:
            self.offTimestamp = now

    def switchOffTiming(self, seconds):
        """ Predict when the lamp is supposed to turn off """
        return self.onTimestamp + seconds

    def elapsed(self):
        """ Return number of seconds since the lamp is actually on. """
        if not self.lampOn:
            return 0

        return pfsTime.timestamp() - self.onTimestamp
