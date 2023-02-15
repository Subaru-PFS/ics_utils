__author__ = 'alefur'

import ics.utils.time as pfsTime

warmingTime = dict(neon=15, xenon=15, krypton=15, argon=15, halogen=15, hgar=60, hgcd=60)
allLamps = list(warmingTime.keys())


class LampState(object):
    # max time a lamp can stay off, without requiring pre-warming again.
    maxTimeIdle = 7200
    # number of seconds of pre-warming time for all lamps but hgar/hgcd.
    preWarmingTime = 5
    """ Handle lamp state and keywords. """

    def __init__(self, name):
        self.name = name
        self.state = 'unknown'
        self.onTimestamp = self.offTimestamp = pfsTime.timestamp()

    @property
    def lampOn(self):
        return self.state == 'on'

    def __str__(self):
        return ','.join([f'{self.state}',
                         f'{pfsTime.Time.fromtimestamp(self.offTimestamp).isoformat()}',
                         f'{pfsTime.Time.fromtimestamp(self.onTimestamp).isoformat()}'])

    def needWarmup(self, now):
        """Does the lamp needs to warmed up during wipe"""
        # short pre-warmup if the lamp has not been used in a long time.
        needWarmupTime = 0 if not self.lampOn and (now - self.offTimestamp) < LampState.maxTimeIdle else LampState.preWarmingTime
        # overriding anyway for hgar, basically trying to crudely imitate hgcd pfilamps logic, never tested...
        needWarmupTime = 60 if self.name in ['hgar', 'hgcd'] else needWarmupTime

        return needWarmupTime

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
