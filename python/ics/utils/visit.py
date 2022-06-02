import logging
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
        """ Reload persisted pfsField. """
        try:
            pfsDesignId, visit0 = self.actor.instData.loadKey('pfsField')
            pfsDesign = PfsDesign.read(int(pfsDesignId, 16), dirName=self.actor.actorConfig['pfsDesign']['rootDir'])
            pfsField = PfsField(pfsDesign, visit0)
        except:
            pfsField = None

        return pfsField

    def declareNewField(self, pfsDesignId):
        """ Declare new field, read pfsDesign and get visit0. """
        self.finishField()

        pfsDesign = PfsDesign.read(pfsDesignId, dirName=self.actor.actorConfig['pfsDesign']['rootDir'])
        visit0 = self._fetchVisitFromGen2(pfsDesignId=pfsDesign.pfsDesignId)

        self.activeField = PfsField(pfsDesign, visit0)
        # persisting pfsField
        self.actor.instData.persistKey('pfsField', '0x%016x' % pfsDesign.pfsDesignId, visit0)

        return pfsDesign, self.activeField.visit0

    def getField(self, consumer):
        """ """
        if self.activeField is None:
            raise RuntimeError('no pfsDesign has been declared current...')

        pfsDesignId = self.activeField.getPfsDesignId()
        visit = self.getVisit(consumer)

        return pfsDesignId, visit

    def finishField(self):
        """ """
        self.activeField = None
        self.actor.instData.persistKey('pfsField', None, None)

    def getVisit(self, consumer, name=None):
        """ Get visit, visit0 if available otherwise new one"""
        if self.activeField and self.activeField.visit0.available(consumer):
            return self.activeField.visit0

        return self.newVisit(consumer, name=name)

    def newVisit(self, consumer, name=None):
        """ Generate new visit. """
        visitId = self._fetchVisitFromGen2()
        self.activeVisit[visitId] = Visit(visitId=visitId, consumer=consumer, name=name)
        return self.activeVisit[visitId]

    def releaseVisit(self, visitId=None, consumer=None):
        """ Release active visit. """
        if visitId is None:
            try:
                [visitId] = list(self.activeVisit.keys())
            except ValueError:
                raise RuntimeError(f'dont know which visit to release : {",".join(map(str, self.activeVisit.keys()))}')

        # we are still doing slightly weird things for now, so I'll play safe
        if visitId not in self.activeVisit.keys():
            logging.warning(f'visitId:{visitId} not in active visits, fine for now...')
            return

        self.activeVisit[visitId].stop()
        self.activeVisit.pop(visitId, None)

    def _fetchVisitFromGen2(self, pfsDesignId=None):
        """Actually get a new visit from Gen2.
        What PFS calls a "visit", Gen2 calls a "frame".
        """
        return fetchVisitFromGen2(self.actor, designId=pfsDesignId)


class Visit(object):
    def __init__(self, visitId, consumer=None, name=None):
        # sps can have concurrent visit, beside resourceManager is already doing the job
        consumer = f'sps{visitId}' if consumer == 'sps' else consumer

        self.visitId = visitId
        self.name = name
        self.consumer = consumer
        self.__agcFrameId = 0
        self.__fpsFrameId = 0
        self.iAmDead = False
        self.idLock = threading.RLock()

    def __str__(self):
        return (f"Visit(name={self.name} consumer={self.consumer} visitId={self.visitId} "
                f"fpsFrame={self.__fpsFrameId} agcFrame={self.__agcFrameId})")

    def stop(self):
        """Declare that we should not be used any more."""
        self.iAmDead = True

    def agcFrameId(self):
        """ AGC frame id accessor. """
        return self.__agcFrameId

    def fpsFrameId(self):
        """ FPS frame id accessor. """
        return self.__fpsFrameId

    def frameForAGC(self):
        if self.iAmDead:
            raise VisitAlreadyDone()

        with self.idLock:
            frameIdx = self.__agcFrameId
            if frameIdx >= 100:
                raise VisitOverflowed()
            self.__agcFrameId += 1

        return self.visitId * 100 + frameIdx

    def frameForFPS(self):
        if self.iAmDead:
            raise VisitAlreadyDone()

        with self.idLock:
            frameIdx = self.__fpsFrameId
            if frameIdx >= 100:
                raise VisitOverflowed()
            self.__fpsFrameId += 1

        return self.visitId * 100 + frameIdx


class VisitO(Visit):
    def __init__(self, visitId):
        Visit.__init__(self, visitId, consumer='pfs', name='visit0')

    def available(self, consumer):
        """Assess if visit0 is available for a given consumer, not sure its quite robust, but it would do OK for now."""
        if consumer == 'sps':
            table = 'sps_visit'
        elif consumer in ['fps', 'mcs']:
            table = 'mcs_exposure'
        elif consumer in ['ag', 'agc']:
            table = 'agc_exposure'
            # not sure it would actually work so returning True for now..
            return True
        else:
            raise ValueError('do not know what to do here ...')

        visitAvailable = not opDB.fetchone(f'select pfs_visit_id from {table} where pfs_visit_id={self.visitId}')
        return visitAvailable

    def releaseVisit(self, consumer):
        """"""
        pass


class PfsField(object):
    """Hold pfsDesign and visit0."""

    def __init__(self, pfsDesign, visitId):
        self.pfsDesign = pfsDesign
        self.visit0 = VisitO(int(visitId))

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
