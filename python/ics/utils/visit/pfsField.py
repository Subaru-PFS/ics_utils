import glob
import logging
import os

import ics.utils.visit.pfsVisit as pfsVisit
import pfs.utils.ingestPfsDesign as ingestPfsDesign
from pfs.datamodel import PfsDesign, PfsConfig, InstrumentStatusFlag


def _iterRecentDateDirs(rawRoot="/data/raw", maxDateDirs=7):
    """Yield up to maxDateDirs most-recent date directories under rawRoot."""
    dateDirs = glob.glob(os.path.join(rawRoot, "20??-??-??"))
    dateDirs = [d for d in dateDirs if os.path.isdir(d)]
    dateDirs.sort(reverse=True)

    for d in dateDirs[:maxDateDirs]:
        yield d


def _findPfsConfigPath(designId, visit0, rawRoot="/data/raw", maxDateDirs=7):
    """Find pfsConfig path by checking up to maxDateDirs recent date directories."""
    fileName = "pfsConfig-0x%016x-%06d.fits" % (designId, visit0)

    for dateDir in _iterRecentDateDirs(rawRoot=rawRoot, maxDateDirs=maxDateDirs):
        candidate = os.path.join(dateDir, "pfsConfig", fileName)
        if os.path.isfile(candidate):
            return candidate, dateDir

    return None, None


class PfsField(object):
    """Hold pfsDesign, visit0, pfsConfig..."""

    def __init__(self, iicActor, pfsDesignId, agV, fpsV, spsV, pfsConfigId=None, visit0=None):
        self.logger = logging.getLogger('pfsField')
        self.iicActor = iicActor
        self.visit = dict(ag=pfsVisit.AgVisit(agV, name='visit0'),
                          fps=pfsVisit.FpsVisit(fpsV, name='visit0'),
                          sps=pfsVisit.SpsVisit(spsV, name='visit0'))

        pfsDesignId = int(pfsDesignId, 16) if isinstance(pfsDesignId, str) else pfsDesignId
        self.pfsDesign = PfsDesign.read(pfsDesignId, dirName=iicActor.actorConfig['pfsDesign']['rootDir'])
        self.pfsConfig0 = None

        pfsConfigId = self.pfsDesignId if pfsConfigId is None else pfsConfigId
        visit0 = self.fpsVisitId if visit0 is None else visit0

        # try to reload pfsConfig as well as it might already exist.
        if visit0:
            self.loadPfsConfig0(pfsConfigId, visit0, doIgnore=True)

    @property
    def fpsVisitId(self):
        return self.visit['fps'].visitId

    @property
    def visit0(self):
        return None if self.pfsConfig0 is None else self.pfsConfig0.visit0

    @property
    def pfsDesignId(self):
        return self.pfsDesign.pfsDesignId

    @property
    def pfsConfigId(self):
        return None if self.pfsConfig0 is None else self.pfsConfig0.pfsDesignId

    @classmethod
    def declareNew(cls, iicActor, pfsDesignId, visit0):
        """Main constructor, called whenever a new design is declared."""
        obj = cls(iicActor, pfsDesignId, *3 * [visit0])
        obj.persist()
        return obj

    @classmethod
    def reload(cls, iicActor):
        """Reload pfsField object when iicActor restart."""
        return cls(iicActor, *iicActor.actorData.loadKey('pfsField'))

    def persist(self):
        """Persist pfsField members to disk."""
        self.iicActor.actorData.persistKey('pfsField',
                                           '0x%016x' % self.pfsDesign.pfsDesignId,
                                           *[self.visit[sub].visitId for sub in ['ag', 'fps', 'sps']],
                                           self.pfsConfigId, self.visit0)

    def getVisit(self, caller):
        """Get visit for caller."""
        return self.visit[caller]

    def isVisitAvailableFor(self, caller):
        """Is visit available for that caller."""
        return self.getVisit(caller).isAvailable

    def reconfigure(self, caller, newVisit):
        """visit got bumped up."""
        self.visit[caller] = newVisit

        # keep sps and ag in sync
        if caller == 'sps':
            self.visit['ag'] = pfsVisit.AgVisit(newVisit.visitId)

        # persist visits to disk.
        self.persist()

    def getGratingPosition(self):
        """
        Return the required red grating position from the PfsDesign.

        Returns
        -------
        str
            The required red grating position, either 'low' or 'med'. If both or neither
            of the positions are present in the PFS design, returns None.
        """
        lowRes = 'r' in self.pfsDesign.arms
        medRes = 'm' in self.pfsDesign.arms

        if lowRes and not medRes:
            position = 'low'
        elif medRes and not lowRes:
            position = 'med'
        else:
            position = None

        return position

    def makePfsConfig(self, visitId, cards, camMask, forcePfsConfig=False):
        """Create and return a new pfsConfig object for this visit."""
        if self.pfsDesign is None:
            raise RuntimeError('no PfsDesign is declared as current')

        if self.pfsConfig0 is None:
            if not forcePfsConfig:
                raise RuntimeError(f'no pfsConfig0 found for the current PfsDesign ({self.pfsDesign.filename})')

            self.logger.info('pfsConfig0 is not available, creating it from current PfsDesign.')
            self.setPfsConfig0(PfsConfig.fromPfsDesign(self.pfsDesign, visit=visitId,
                                                       pfiCenter=self.pfsDesign.pfiNominal))
            ingestPfsDesign.ingestPfsConfig(self.pfsConfig0)

            # we do not want fps visit to fall behind.
            if self.fpsVisitId < self.pfsConfig0.visit:
                self.reconfigure('fps', newVisit=pfsVisit.FpsVisit(visitId, name='visit0'))

        if self.pfsConfig0.pfsDesignId != self.pfsDesignId:
            if not forcePfsConfig:
                raise RuntimeError(
                    f'pfsConfig0 ({self.pfsConfig0.filename}) does not match the current PfsDesign ({self.pfsDesign.filename})'
                )
            self.logger.info(
                f'forcePfsConfig=True proceeding even though pfsConfig0 ({self.pfsConfig0.filename}) does not match the current PfsDesign ({self.pfsDesign.filename})'
            )

        if self.pfsConfig0.instStatusFlag & InstrumentStatusFlag.CONVERGENCE_FAILED:
            if not forcePfsConfig:
                raise RuntimeError(
                    f'Cannot create pfsConfig: CONVERGENCE_FAILED bit is set '
                    f'for pfsConfig-0x{self.pfsConfig0.pfsDesignId:016x}-{self.pfsConfig0.visit:06d}'
                )
            self.logger.info(
                'forcePfsConfig=True proceeding even though CONVERGENCE_FAILED bit is set'
            )

        return self.pfsConfig0.copy(visit=visitId, header=cards, camMask=camMask, visit0=self.pfsConfig0.visit)

    def loadPfsConfig0(self, designId, visit0, doIgnore=False, rawRoot="/data/raw", maxDateDirs=7):
        """Load pfsConfig file after fps convergence."""
        pfsConfigPath, dateDir = _findPfsConfigPath(designId, visit0, rawRoot=rawRoot, maxDateDirs=maxDateDirs)

        if pfsConfigPath is None:
            msg = f"pfsConfig0 not found for designId=0x{designId:016x} visit0={visit0:06d} in last {maxDateDirs} dates"
            if doIgnore:
                self.logger.warning(msg)
                return

            raise FileNotFoundError(msg)

        self.logger.info(f'loading pfsConfig0 from {pfsConfigPath}')
        try:
            self.setPfsConfig0(PfsConfig.read(designId, visit0, dirName=os.path.join(dateDir, 'pfsConfig')))
        except Exception as e:
            self.logger.warning(str(e), exc_info=True)

    def setPfsConfig0(self, pfsConfig0):
        """Same pfsDesign was re-declared, hold on to the latest pfsConfig0"""
        if pfsConfig0 is None:
            self.logger.warning('resetting pfsConfig0...')

        else:
            self.logger.info(
                f'Setting current pfsConfig0 : pfsConfig-0x%016x-%06d.fits' % (pfsConfig0.pfsDesignId,
                                                                               pfsConfig0.visit))
        self.pfsConfig0 = pfsConfig0
        self.persist()

    def lockVisit(self):
        """The current visit won't be available anymore."""
        for caller, visit in self.visit.items():
            self.logger.debug(f'locking visit{visit} for caller:{caller}')
            visit.lock()

    def getVisit0(self):
        """Get current visit0"""
        if self.pfsConfig0 is None:
            raise ValueError('no pfsConfig0 is available.')

        return self.pfsConfig0.visit
