import glob
import logging
import os

import ics.utils.visit.pfsVisit as pfsVisit
import pfs.utils.ingestPfsDesign as ingestPfsDesign
from pfs.datamodel import PfsDesign, PfsConfig


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

    def __init__(self, iicActor, pfsDesignId, agV, fpsV, spsV):
        self.logger = logging.getLogger('pfsField')
        self.iicActor = iicActor
        self.visit = dict(ag=pfsVisit.AgVisit(agV, name='visit0'),
                          fps=pfsVisit.FpsVisit(fpsV, name='visit0'),
                          sps=pfsVisit.SpsVisit(spsV, name='visit0'))

        pfsDesignId = int(pfsDesignId, 16) if isinstance(pfsDesignId, str) else pfsDesignId
        self.pfsDesign = PfsDesign.read(pfsDesignId, dirName=iicActor.actorConfig['pfsDesign']['rootDir'])
        self.pfsConfig0 = None

        # try to reload pfsConfig as well as it might already exist.
        self.loadPfsConfig0(self.pfsDesignId, self.fpsVisitId, doIgnore=True)

    @property
    def fpsVisitId(self):
        return self.visit['fps'].visitId

    @property
    def visit0(self):
        return self.pfsConfig0.visit if self.pfsConfig0 else self.fpsVisitId

    @property
    def pfsDesignId(self):
        return self.pfsDesign.pfsDesignId

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
                                           *[self.visit[sub].visitId for sub in ['ag', 'fps', 'sps']])

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

    def makePfsConfig(self, visitId, cards, camMask):
        """Create and return a new pfsConfig object for this visit."""
        # no pfsConfig0 means that there is no matching fps.pfsConfig, so create it from pfsDesign.
        if self.pfsConfig0 is None:
            self.logger.info('pfsConfig0 is not available, creating it from current PfsDesign.')
            self.pfsConfig0 = PfsConfig.fromPfsDesign(self.pfsDesign, visit=visitId,
                                                      pfiCenter=self.pfsDesign.pfiNominal)
            ingestPfsDesign.ingestPfsConfig(self.pfsConfig0)

            # we do not want fps visit to fall behind.
            if self.fpsVisitId != self.pfsConfig0.visit:
                self.reconfigure('fps', newVisit=pfsVisit.FpsVisit(visitId, name='visit0'))

        return self.pfsConfig0.copy(visit=visitId, header=cards, camMask=camMask, visit0=self.pfsConfig0.visit)

    def loadPfsConfig0(self, designId, visit0, doIgnore=False, rawRoot="/data/raw", maxDateDirs=7):
        """Load pfsConfig file after fps convergence."""
        if designId != self.pfsDesignId:
            return

        pfsConfigPath, dateDir = _findPfsConfigPath(designId, visit0, rawRoot=rawRoot, maxDateDirs=maxDateDirs)

        if pfsConfigPath is None:
            msg = f"pfsConfig0 not found for designId=0x{designId:016x} visit0={visit0:06d} in last {maxDateDirs} dates"
            if doIgnore:
                self.logger.warning(msg)
                return

            raise FileNotFoundError(msg)

        self.logger.info(f'loading pfsConfig0 from {pfsConfigPath}')
        try:
            self.pfsConfig0 = PfsConfig.read(designId, visit0, dirName=os.path.join(dateDir, 'pfsConfig'))
        except Exception as e:
            self.logger.warning(str(e), exc_info=True)

    def holdPfsConfig0(self, pfsConfig0):
        """Same pfsDesign was re-declared, hold on to the latest pfsConfig0"""
        # no need to go further.
        if pfsConfig0 is None:
            return

        self.logger.info(
            f'holding pfsConfig0 from pfsConfig-0x%016x-%06d.fits' % (pfsConfig0.pfsDesignId, pfsConfig0.visit))
        self.pfsConfig0 = pfsConfig0

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
