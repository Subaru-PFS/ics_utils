import glob
import os
import threading

from ics.utils.opdb import opDB
from pfs.datamodel import PfsDesign, PfsConfig
from pfscore.gen2 import fetchVisitFromGen2


class VisitActiveError(Exception):
    pass


class VisitNotActiveError(Exception):
    pass


class VisitOverflowed(Exception):
    pass


class VisitAlreadyDone(Exception):
    pass


class VisitManager(object):
    def __init__(self, actor):
        self.actor = actor
        self.activeField = self.reloadField()

        self.activeVisit = dict()

    def reloadField(self):
        """Reload persisted pfsField."""
        try:
            pfsField = PfsField.reload(self.actor)
        except:
            pfsField = None

        return pfsField

    def declareNewField(self, pfsDesignId):
        """Declare new field, read pfsDesign and get visit0."""
        self.finishField()

        visit0 = self._fetchVisitFromGen2(pfsDesignId=pfsDesignId)
        self.activeField = PfsField.declareNew(self.actor, pfsDesignId, visit0)

        return self.activeField.pfsDesign, self.activeField.visit0

    def getField(self):
        """Get active field."""
        if self.activeField is None:
            raise RuntimeError('no pfsDesign has been declared current...')

        return self.activeField

    def finishField(self):
        """Finish declaredCurrentPfsDesign."""
        self.activeField = None

    def getCurrentDesignId(self):
        """Get declaredCurrentPfsDesignId."""
        return self.getField().pfsDesignId

    def getVisit(self, caller, name=None):
        """Get visit, visit0 if available otherwise new one."""
        if self.activeField:
            if not self.activeField.visitAvailableFor(caller):
                new = self.newVisit(caller, name=name)
                self.activeField.reconfigure(caller=caller, newVisit=new)

            return self.activeField.getVisit(caller)

        return self.newVisit(caller, name=name)

    def newVisit(self, caller, name=None):
        """Generate new visit."""
        visitId = self._fetchVisitFromGen2()
        return Visit.fromCaller(visitId, caller, name=name)

    def _fetchVisitFromGen2(self, pfsDesignId=None):
        """Actually get a new visit from Gen2.
        What PFS calls a "visit", Gen2 calls a "frame".
        """
        return fetchVisitFromGen2(self.actor, designId=pfsDesignId)


class Visit(object):
    exposureTable = ""

    def __init__(self, visitId, caller='iic', name=None):
        self.visitId = visitId
        self.caller = caller
        self.name = name

        self.isActive = False
        self.isFrozen = False
        self.iAmDead = False

        self.__frameId = 0
        self.idLock = threading.RLock()

    def __str__(self):
        return f"Visit(name={self.name} caller={self.caller} visitId={self.visitId} subVisit={self.__frameId}"

    def __enter__(self):
        """Context manager on with statement."""
        self.lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.unlock()

    @property
    def isPopulated(self):
        return opDB.fetchone(f'select pfs_visit_id from {self.exposureTable} where pfs_visit_id={self.visitId}')

    @property
    def isAvailable(self):
        """Assess if visit0 is available for a given caller, not sure its quite robust, but it would do OK for now."""
        used = self.isActive or (self.isPopulated and not self.isFrozen)
        return not used

    @staticmethod
    def fromCaller(visitId, caller, name):
        if caller == 'sps':
            return SpsVisit(visitId, name)
        elif caller in ['mcs', 'fps']:
            return FpsVisit(visitId, name)
        elif caller in ['ag', 'agc']:
            return AgVisit(visitId, name)
        else:
            raise ValueError(f'unknown caller:{caller}')

    def nextFrameId(self):
        """Get subvisit frameId."""
        if self.iAmDead:
            raise VisitAlreadyDone()

        with self.idLock:
            frameIdx = self.__frameId
            if frameIdx >= 100:
                raise VisitOverflowed()
            self.__frameId += 1

        return self.visitId * 100 + frameIdx

    def frameId(self):
        """Frame id accessor."""
        return self.__frameId

    def lock(self):
        """Declare visit active."""
        self.isActive = True

    def unlock(self):
        """Declare visit inactive."""
        self.isActive = False

    def stop(self):
        """Declare that we should not be used any more."""
        self.iAmDead = True

    def setFrozen(self, doFreeze=True):
        """Set visit frozen."""
        self.isFrozen = doFreeze


class PfsField(object):
    """Hold pfsDesign, visit0, pfsConfig..."""

    def __init__(self, iicActor, pfsDesignId, agV, fpsV, spsV):
        self.iicActor = iicActor
        self.visit = dict(ag=AgVisit(agV, name='visit0'),
                          fps=FpsVisit(fpsV, name='visit0'),
                          sps=SpsVisit(spsV, name='visit0'))

        pfsDesignId = int(pfsDesignId, 16) if isinstance(pfsDesignId, str) else pfsDesignId
        self.pfsDesign = PfsDesign.read(pfsDesignId, dirName=iicActor.actorConfig['pfsDesign']['rootDir'])

        # try to reload pfsConfig as well, it might already exist.
        try:
            self.pfsConfig, _ = self.loadPfsConfig()
        except:
            self.pfsConfig = None

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

    def visitAvailableFor(self, caller):
        """Is visit available for that caller."""
        return self.getVisit(caller).isAvailable

    def reconfigure(self, caller, newVisit):
        """visit got bumped up."""
        self.visit[caller] = newVisit

        # keep sps and ag in sync
        if caller == 'sps':
            self.visit['ag'] = AgVisit(newVisit.visitId)

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

    def loadPfsConfig(self):
        """Load pfsConfig file after fps convergence."""
        [pfsConfigPath] = glob.glob('/data/raw/*-*-*/pfsConfig/pfsConfig-0x%016x-%06d.fits' % (self.pfsDesignId,
                                                                                               self.visit0))
        dirName, _ = os.path.split(pfsConfigPath)
        rootDir, _ = os.path.split(dirName)
        _, dateDir = os.path.split(rootDir)

        self.pfsConfig = PfsConfig.read(self.pfsDesignId, self.visit0, dirName=dirName)
        return self.pfsConfig, dateDir


class AgVisit(Visit):
    exposureTable = 'agc_exposure'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'ag', name=name)
        self.isFrozen = True


class FpsVisit(Visit):
    exposureTable = 'mcs_exposure'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'fps', name=name)


class SpsVisit(Visit):
    exposureTable = 'sps_visit'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'sps', name=name)

    @property
    def isAvailable(self):
        """We actually always bump up sps after field acquisition."""
        return False
