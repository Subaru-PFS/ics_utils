import logging
import threading

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
        self.activeVisit = dict()
        self.visit0 = None

    @property
    def validVisit0(self):
        """ Allocate a new visit. """
        return self.visit0 is not None

    def declareNewField(self, designId):
        """ Allocate a new visit. """
        visitId = self._fetchVisitFromGen2(designId=designId)
        self.visit0 = VisitO(visitId)
        return self.visit0

    def resetVisit0(self):
        """ Allocate a new visit. """
        if self.visit0 is not None:
            logging.warning(f'resetting visit0 : {str(self.visit0)}')

        self.visit0 = None

    def getVisit(self, consumer, name=None):
        """ Allocate a new visit. """
        if self.validVisit0 and self.visit0.getVisit(consumer):
            return self.visit0

        return self.newVisit(consumer, name=name)

    def newVisit(self, consumer, name=None):
        """ Allocate a new visit. """
        # if self.perConsumer(consumer) is not None:
        #     raise VisitActiveError()

        visit = self._fetchVisitFromGen2()
        self.activeVisit[visit] = Visit(visitId=visit, consumer=consumer, name=name)
        return self.activeVisit[visit]

    def releaseVisit(self, visit=None, consumer=None):
        """ Allocate a new visit. """
        if visit is None:
            try:
                [visit] = list(self.activeVisit.keys())
            except ValueError:
                raise RuntimeError(f'dont know which visit to release : {",".join(map(str, self.activeVisit.keys()))}')

        if self.validVisit0 and visit == self.visit0.visitId:
            return

        if self.activeVisit[visit] is None:
            raise VisitNotActiveError()

        self.activeVisit[visit].stop()
        self.activeVisit.pop(visit, None)

    def _fetchVisitFromGen2(self, designId=None):
        """Actually get a new visit from Gen2.
        What PFS calls a "visit", Gen2 calls a "frame".
        """
        return fetchVisitFromGen2(self.actor, designId=designId)


class Visit(object):
    def __init__(self, visitId, consumer=None, name=None):
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
    def __init__(self, visit, consumer='fps'):
        Visit.__init__(self, visit, consumer=consumer)
        self.consumers = dict(sps=True, agc=True)

    def getVisit(self, consumer):
        """ """
        if self.consumers[consumer]:
            self.consumers[consumer] = False
            return True

        return False
