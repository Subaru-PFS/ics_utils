import threading

from ics.utils.opdb import opDB
from pfs.datamodel import PfsDesign
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
            pfsDesignId, visit0 = self.actor.instData.loadKey('pfsField')
            pfsDesign = PfsDesign.read(int(pfsDesignId, 16), dirName=self.actor.actorConfig['pfsDesign']['rootDir'])
            pfsField = PfsField(pfsDesign, visit0)
        except:
            pfsField = None

        return pfsField

    def declareNewField(self, pfsDesignId):
        """Declare new field, read pfsDesign and get visit0."""
        self.finishField()

        pfsDesign = PfsDesign.read(pfsDesignId, dirName=self.actor.actorConfig['pfsDesign']['rootDir'])
        visit0 = self._fetchVisitFromGen2(pfsDesignId=pfsDesign.pfsDesignId)

        self.activeField = PfsField(pfsDesign, visit0)
        # persisting pfsField
        self.actor.instData.persistKey('pfsField', '0x%016x' % pfsDesign.pfsDesignId, visit0)

        return pfsDesign, self.activeField.visit0

    def getField(self):
        """Get active field."""
        if self.activeField is None:
            raise RuntimeError('no pfsDesign has been declared current...')

        return self.activeField

    def finishField(self):
        """Finish declaredCurrentPfsDesign."""
        self.activeField = None
        self.actor.instData.persistKey('pfsField', None, None)

    def getCurrentDesignId(self):
        """Get declaredCurrentPfsDesignId."""
        return self.getField().getPfsDesignId()

    def getVisit(self, caller, name=None):
        """Get visit, visit0 if available otherwise new one."""
        if self.activeField and self.activeField.visitAvailableFor(caller):
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
    isActive = False
    isFrozen = False
    exposureTable = ""

    def __init__(self, visitId, caller='iic', name=None):
        self.visitId = visitId
        self.caller = caller
        self.name = name

        self.isActive = False
        self.iAmDead = False
        self.__frameId = 0
        self.idLock = threading.RLock()

    def __str__(self):
        return f"Visit(name={self.name} caller={self.caller} visitId={self.visitId} subVisit={self.__frameId}"

    def __enter__(self):
        """Context manager on with statement."""
        self.isActive = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.isActive = False

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

    def stop(self):
        """Declare that we should not be used any more."""
        self.iAmDead = True

    def setFrozen(self, doFreeze=True):
        """Set visit frozen."""
        self.isFrozen = doFreeze


class PfsField(object):
    """Hold pfsDesign and visit0."""

    def __init__(self, pfsDesign, visit0):
        self.pfsDesign = pfsDesign
        self.visit0 = visit0

        self.visit = dict(ag=AgVisit(visit0, name='visit0'),
                          fps=FpsVisit(visit0, name='visit0'),
                          sps=SpsVisit(visit0, name='visit0'))

    def getVisit(self, caller):
        """Get visit for caller."""
        return self.visit[caller]

    def visitAvailableFor(self, caller):
        """Is visit available for that caller."""
        return self.getVisit(caller).isAvailable

    def getPfsDesignId(self):
        """Return current pfsDesignId."""
        return self.pfsDesign.pfsDesignId

    def getGratingPosition(self):
        """Return required red grating position from pfsDesign."""
        if 'r' in self.pfsDesign.arms:
            position = 'low'
        elif 'm' in self.pfsDesign.arms:
            position = 'med'
        else:
            position = None

        return position


class AgVisit(Visit):
    isFrozen = True
    exposureTable = 'agc_exposure'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'ag', name=name)


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
