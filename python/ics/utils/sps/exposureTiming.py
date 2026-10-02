"""Exposure timing model shared by spsActor and iicActor.

spsActor sizes its H4 ramps with it, iicActor budgets the lamps it keeps lit through a sequence
of exposures. The parameters live in the exposure section of pfs_instdata config/actors/sps.yaml.

All times are in seconds and counted from the moment sps receives the expose command. H4 read
time and IRP ratio are read from the hx actor models, as they follow its IRP mode.
"""
import logging

import ics.utils.instdata.io as instdataIO

logger = logging.getLogger('exposureTiming')


class ExposureTiming(object):
    def __init__(self, exposureConfig):
        """Exposure timing model.

        Parameters
        ----------
        exposureConfig : `dict`
            exposure section of sps.yaml, holding the ramp and timing sections.
        """
        self.ramp = exposureConfig['ramp']
        self.timing = exposureConfig['timing']

    @classmethod
    def fromInstdata(cls):
        """Load the model from pfs_instdata sps.yaml."""
        return cls(instdataIO.loadConfig('sps', subDirectory='actors')['sps']['exposure'])

    @staticmethod
    def hasCcd(arms):
        """Whether any of the arms is read by a ccd."""
        return bool(set(arms) - {'n'})

    @staticmethod
    def hasNir(arms):
        """Whether any of the arms is read by an H4."""
        return 'n' in arms

    @staticmethod
    def readReadTime(hxModel):
        """H4 read time, from its hx actor model."""
        return float(hxModel.keyVarDict['readTime'].getValue())

    def readIrpRatio(self, hxModel):
        """IRP ratio the H4 reads with, from its hx actor model.

        hx publishes irp along with readTime, with a ratio of 0 when IRP is disabled, so the
        configured ratio is only a safety net, for an H4 reading without IRP or an irp not
        published; it is logged when used.
        """
        irp = hxModel.keyVarDict.get('irp')
        enabled, irpRatio, irpOffset = irp.getValue(doRaise=False) if irp is not None else 3 * (None,)

        if not irpRatio:
            state = 'not published' if irpRatio is None else 'disabled'
            irpRatio = self.ramp['irpRatio']
            logger.warning(f'{hxModel.actor} irp is {state}, assuming irpRatio={irpRatio}')

        return irpRatio

    def nExtraRead(self, irpRatio):
        """Reads added to an H4 ramp to cover the spread in start-up between modules."""
        return self.ramp['nExtraReadPerIrp'][irpRatio]

    def finalReadAtCcdRows(self, irpRatio):
        """Fraction of ccd rows read out when the H4 is told its next read is the last."""
        return self.ramp['finalReadAtCcdRows'][irpRatio]

    def nH4Read(self, exptime, readTime, irpRatio, hasCcd=False, overhead=0):
        """Number of reads of an illuminated H4 ramp.

        The ramp brackets the exposure with clean reads, and reads through the ccd readout
        when ccds are exposed, in which case it is ended with them.

        Parameters
        ----------
        exptime : `float`
            exposure time.
        readTime : `float`
            H4 read time.
        irpRatio : `int`
            IRP ratio.
        hasCcd : `bool`
            whether ccds are exposed alongside.
        overhead : `float`
            extra time the ramp has to cover.
        """
        readThrough = self.timing['ccdReadTime'] if hasCcd else 0
        nReadMin = self.ramp['nReadMin'] + self.nExtraRead(irpRatio)
        return int((exptime + overhead + readThrough) // readTime + nReadMin)

    def h4ReadDoneTime(self, readTime, nRead):
        """When the slowest H4 reports its nth read.

        After a fixed start-up, the ASIC waits up to one read for its frame, then takes the
        reset frame, then read 1.
        """
        return self.timing['h4StartupTime'] + (2 + nRead) * readTime

    def shutterOpenTime(self, arms, readTime=None):
        """When the shutters are open, one trip after the ccds are wiped and the H4s have done their first read."""
        ready = [self.timing['ccdWipeTime']] if self.hasCcd(arms) else []
        ready += [self.h4ReadDoneTime(readTime, 1)] if self.hasNir(arms) else []
        return max(ready) + self.timing['shutterTransientTime']

    def shutterCloseTime(self, exptime, arms, readTime=None):
        """When the shutters are closed, exptime after they opened as both trips take as long."""
        return self.shutterOpenTime(arms, readTime=readTime) + exptime

    def ccdEndTime(self, exptime, arms, readTime=None):
        """When the last ccd read is done, 0 without ccds."""
        if not self.hasCcd(arms):
            return 0

        return self.shutterCloseTime(exptime, arms, readTime=readTime) + self.timing['ccdReadTime']

    def h4EndTime(self, exptime, arms, readTime=None, irpRatio=None):
        """When the last H4 ramp is done, 0 without H4s.

        The final read is declared at shutter close, or once the ccds have read out
        finalReadAtCcdRows of their rows; the ramp then ends within two reads, and never later
        than its planned length.
        """
        if not self.hasNir(arms):
            return 0

        hasCcd = self.hasCcd(arms)
        nRead = self.nH4Read(exptime, readTime, irpRatio, hasCcd=hasCcd)
        shutterClose = self.shutterCloseTime(exptime, arms, readTime=readTime)
        ccdRowsTime = self.finalReadAtCcdRows(irpRatio) * self.timing['ccdReadTime'] if hasCcd else 0

        return min(self.h4ReadDoneTime(readTime, nRead), shutterClose + ccdRowsTime + 2 * readTime)

    def exposureTime(self, exptime, arms, readTime=None, irpRatio=None):
        """How long an exposure takes, until the next one can be commanded.

        Parameters
        ----------
        exptime : `float`
            exposure time.
        arms : iterable of `str`
            exposed arms, among b, r, m and n.
        readTime : `float`
            H4 read time, required with the n arm.
        irpRatio : `int`
            IRP ratio, required with the n arm.
        """
        detectorsEnd = max(self.ccdEndTime(exptime, arms, readTime=readTime),
                           self.h4EndTime(exptime, arms, readTime=readTime, irpRatio=irpRatio))
        return detectorsEnd + self.timing['exposureMargin']

    def lampOnTime(self, exptime, arms, readTime=None, irpRatio=None, duplicate=1):
        """How long a lamp lit for a whole sequence must burn, until the last shutter close.

        Parameters
        ----------
        exptime : `float`
            exposure time.
        arms : iterable of `str`
            exposed arms, among b, r, m and n.
        readTime : `float`
            H4 read time, required with the n arm.
        irpRatio : `int`
            IRP ratio, required with the n arm.
        duplicate : `int`
            number of exposures in the sequence.
        """
        exposureTime = self.exposureTime(exptime, arms, readTime=readTime, irpRatio=irpRatio)
        lastShutterClose = self.shutterCloseTime(exptime, arms, readTime=readTime) + self.timing['exposureMargin']
        return (duplicate - 1) * exposureTime + lastShutterClose
