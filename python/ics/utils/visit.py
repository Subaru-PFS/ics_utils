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
        self.activeVisit = None

    def __enter__(self):
        return self.newVisit()

    def __exit__(self):
        self.releaseVisit()

    def newVisit(self, name=None):
        """ Allocate a new visit. """

        if self.activeVisit is not None:
            raise VisitActiveError()
        visit = self._fetchVisitFromGen2()

        self.activeVisit = Visit(visitId=visit, name=name)

        return self.activeVisit

    def releaseVisit(self):
        if self.activeVisit is None:
            raise VisitNotActiveError()

        self.activeVisit.stop()
        self.activeVisit = None

    def _fetchVisitFromGen2(self):
        """Actually get a new visit from Gen2.
        What PFS calls a "visit", Gen2 calls a "frame".
        """
        return fetchVisitFromGen2(self.actor)


class Visit(object):
    def __init__(self, visitId, name=None):
        self.visitId = visitId
        self.name = name
        self.__agcFrameId = 0
        self.__fpsFrameId = 0
        self.iAmDead = False
        self.idLock = threading.RLock()

    def __str__(self):
        return (f"Visit(name={self.name} visitId={self.visitId} "
                f"fpsFrame={self.__fpsFrameId} agcFrame={self.__agcFrameId})")

    def stop(self):
        """Declare that we should not be used any more."""

        self.iAmDead = True

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
