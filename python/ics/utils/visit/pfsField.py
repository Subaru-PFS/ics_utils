import glob
import logging
import os

import ics.utils.visit.pfsVisit as pfsVisit
import numpy as np
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

    @staticmethod
    def formatPfsConfig0Id(pfsDesignId, visit0):
        """Return canonical pfsConfig0 identifier string from designId and visit."""
        return f'pfsConfig0-0x{pfsDesignId:016x}-{visit0:06d}'

    def getPfsConfig0Id(self, pfsConfig0=None):
        """Return canonical pfsConfig0 identifier for current or given pfsConfig0."""
        pfsConfig0 = self.pfsConfig0 if pfsConfig0 is None else pfsConfig0
        return self.formatPfsConfig0Id(pfsConfig0.pfsDesignId, pfsConfig0.visit)

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

    def makePfsConfig(self, visitId, cards, camMask, forcePfsConfig=False, versions=None, isPfiExposure=False):
        """Create and return a new pfsConfig for this visit.

        If no pfsConfig0 exists, one is bootstrapped from the current PfsDesign.
        For PFI (science) exposures, pfsConfig0 is validated against the current
        design and convergence status before proceeding.

        Parameters
        ----------
        visitId : `int`
            Visit ID for the new pfsConfig.
        cards : FITS header
            Header cards to embed in the pfsConfig.
        camMask : `int`
            Bitmask of selected cameras.
        forcePfsConfig : `bool`, optional
            If True, bypass pfsConfig0 validation checks and proceed with warnings.
        versions : `dict`, optional
            Software versions to embed.
        isPfiExposure : `bool`, optional
            True for science (PFI) exposures, which require a valid pfsConfig0
            from a prior FPS convergence.

        Returns
        -------
        pfsConfig : `PfsConfig`
            New pfsConfig copied from pfsConfig0 for the given visit.

        Raises
        ------
        RuntimeError
            If no PfsDesign is declared, or if pfsConfig0 validation fails
            and forcePfsConfig is False.
        """
        if self.pfsDesign is None:
            raise RuntimeError('no PfsDesign is declared as current')

        if self.pfsConfig0 is None:
            self.pfsConfig0 = self._bootstrapPfsConfig0(visitId, versions, isPfiExposure, forcePfsConfig)
            ingestPfsDesign.ingestPfsConfig(self.pfsConfig0)

        if isPfiExposure:
            self._validatePfsConfig0(camMask, forcePfsConfig)

        return self.pfsConfig0.copy(visit=visitId, header=cards, camMask=camMask,
                                    visit0=self.pfsConfig0.visit, versions=versions)

    def _bootstrapPfsConfig0(self, visitId, versions, isPfiExposure, forcePfsConfig):
        """Create pfsConfig0 from scratch when none exists.

        For non-PFI (engineering) exposures, pfiNominal is used as fiber positions.
        For PFI (science) exposures, FPS convergence should have produced a pfsConfig0
        already — bootstrapping is only allowed when forcePfsConfig=True, in which case
        CONVERGENCE_SKIPPED is set and fiber positions are left as NaN.

        Parameters
        ----------
        visitId : `int`
            Visit ID for the new pfsConfig0.
        versions : `dict`
            Software versions to embed.
        isPfiExposure : `bool`
            True for science (PFI) exposures.
        forcePfsConfig : `bool`
            If True, allow bootstrapping even for PFI exposures without a prior convergence.

        Raises
        ------
        RuntimeError
            If isPfiExposure is True and forcePfsConfig is False.
        """
        if isPfiExposure and not forcePfsConfig:
            raise RuntimeError(f'no pfsConfig0 found for the current PfsDesign ({self.pfsDesign.filename})')

        self.logger.info('pfsConfig0 is not available, bootstrapping from current PfsDesign.')
        pfsConfig0 = PfsConfig.fromPfsDesign(self.pfsDesign, visit=visitId,
                                             pfiCenter=self.pfsDesign.pfiNominal.copy(), versions0=versions)

        if isPfiExposure:
            # No convergence was performed: flag it and clear fiber positions.
            pfsConfig0.setInstrumentStatusFlag(InstrumentStatusFlag.CONVERGENCE_SKIPPED)
            pfsConfig0.pfiCenter[:] = np.nan

            # Keep fps visit in sync when bootstrapping ahead of convergence.
            if self.fpsVisitId < visitId:
                self.reconfigure('fps', newVisit=pfsVisit.FpsVisit(visitId, name='visit0'))

        return pfsConfig0

    def _validatePfsConfig0(self, camMask, forcePfsConfig):
        """Validate pfsConfig0 against the current design and exposure requirements.

        Checks that pfsConfig0 matches the current designId, has no blocking status
        flags (CONVERGENCE_FAILED, CONVERGENCE_SKIPPED), and covers the requested arms.
        Each failed check either raises or logs a warning depending on forcePfsConfig.

        Parameters
        ----------
        camMask : `int`
            Bitmask of selected cameras, used to derive the required arms.
        forcePfsConfig : `bool`
            If True, log warnings instead of raising on failed checks.
        """

        def check(condition, reason):
            if condition:
                msg = f'{self.getPfsConfig0Id()} : {reason}'
                if not forcePfsConfig:
                    raise RuntimeError(f'{msg}, use forcePfsConfig=True to proceed.')
                self.logger.info(f'{msg}, forcePfsConfig=True proceeding anyway.')

        check(self.pfsConfig0.pfsDesignId != self.pfsDesignId,
              f'does not match the current PfsDesign ({self.pfsDesign.filename})')
        check(bool(self.pfsConfig0.instStatusFlag & InstrumentStatusFlag.CONVERGENCE_FAILED),
              'CONVERGENCE_FAILED bit is set')
        check(bool(self.pfsConfig0.instStatusFlag & InstrumentStatusFlag.CONVERGENCE_SKIPPED),
              'CONVERGENCE_SKIPPED bit is set')

        selectedArms = {cam[0] for cam in PfsConfig.toCameraList(camMask)}
        diffArm = selectedArms - set(self.pfsConfig0.arms)
        check(bool(diffArm), f"{','.join(diffArm)} not present in pfsConfig.arms")

    def loadPfsConfig0(self, designId, visit0, doIgnore=False, rawRoot="/data/raw", maxDateDirs=7):
        """Load pfsConfig file after fps convergence."""
        pfsConfigPath, dateDir = _findPfsConfigPath(designId, visit0, rawRoot=rawRoot, maxDateDirs=maxDateDirs)

        if pfsConfigPath is None:
            msg = f"{self.formatPfsConfig0Id(designId, visit0)} not found in last {maxDateDirs} dates"
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
            self.logger.info(f'Setting current pfsConfig0 : {self.getPfsConfig0Id(pfsConfig0)}')

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
