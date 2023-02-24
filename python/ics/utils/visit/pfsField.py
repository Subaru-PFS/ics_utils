import glob
import os
import logging
import ics.utils.visit.pfsVisit as pfsVisit
import pfs.utils.ingestPfsDesign as ingestPfsDesign
from pfs.datamodel import PfsDesign, PfsConfig


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

        # try to reload pfsConfig as well, it might already exist.
        try:
            self.pfsConfig0 = self.loadPfsConfig0(self.pfsDesignId, self.visit0)
        except:
            self.pfsConfig0 = None

    @property
    def visit0(self):
        return self.visit['fps'].visitId

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
        """Return required red grating position from pfsDesign."""
        if 'r' in self.pfsDesign.arms:
            position = 'low'
        elif 'm' in self.pfsDesign.arms:
            position = 'med'
        else:
            position = None

        return position

    def getPfsConfig(self, visitId, cards):
        """Create and return a new pfsConfig object for this visit."""
        # if there is no pfsConfig0, meaning that there is no matching fps.pfsConfig, so create it from pfsDesign.
        if self.pfsConfig0 is None:
            self.logger.info('pfsConfig0 is not available, creating it from current PfsDesign.')
            self.pfsConfig0 = PfsConfig.fromPfsDesign(self.pfsDesign, visit=visitId, pfiCenter=self.pfsDesign.pfiNominal)
            ingestPfsDesign.ingestPfsConfig(self.pfsConfig0)

            # make sure that fpsVisit stay in sync with pfsConfig0.visit.
            if self.visit0 != self.pfsConfig0.visit:
                self.reconfigure('fps', newVisit=pfsVisit.FpsVisit(visitId, name='visit0'))

        return self.pfsConfig0.copy(visit=visitId, header=cards)

    def loadPfsConfig0(self, designId, visit0):
        """Load pfsConfig file after fps convergence."""
        # if designId or visit0 does not match, do not anything.
        if designId != self.pfsDesignId or visit0 != self.visit0:
            return

        [pfsConfigPath] = glob.glob('/data/raw/*-*-*/pfsConfig/pfsConfig-0x%016x-%06d.fits' % (self.pfsDesignId,
                                                                                               self.visit0))
        dirName, _ = os.path.split(pfsConfigPath)
        self.logger.info(f'loading pfsConfig0 from {pfsConfigPath}')
        self.pfsConfig0 = PfsConfig.read(self.pfsDesignId, self.visit0, dirName=dirName)

        return self.pfsConfig0
