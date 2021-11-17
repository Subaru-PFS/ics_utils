import threading

from ics.utils.opdb import opDB
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
        self.visit0 = self.reloadVisit0()

    @property
    def validVisit0(self):
        """ Allocate a new visit. """
        return self.visit0 is not None

    def reloadVisit0(self):
        """ Reload persisted visit0. """
        try:
            visitId, = self.actor.instData.loadKey('visit0')
            visit0 = VisitO(visitId)
            visit0.stop()  # reloading but dont use actually use it
        except:
            visit0 = None

        return visit0

    def declareNewField(self, designId):
        """ Declare new field, reset existing visit0 and set a new one. """
        self.resetVisit0()

        visitId = self._fetchVisitFromGen2(designId=designId)
        self.visit0 = VisitO(visitId)
        self.actor.instData.persistKey('visit0', visitId)

        return self.visit0

    def resetVisit0(self, cmd=None):
        """ reset existing visit0. """
        cmd = self.actor.bcast if cmd is None else cmd

        if self.visit0 is not None:
            self.visit0.reset(cmd)
            cmd.warn(f'text="resetting visit0 : {str(self.visit0)}"')

        self.actor.instData.persistKey('visit0', None)
        self.visit0 = None

    def getVisit(self, consumer, name=None):
        """ Get visit, visit0 if available otherwise new one"""
        if self.validVisit0 and self.visit0.getVisit(consumer):
            return self.visit0

        return self.newVisit(consumer, name=name)

    def newVisit(self, consumer, name=None):
        """ Generate new visit. """
        if consumer in self.gatherConsumers():
            raise VisitActiveError()

        visit = self._fetchVisitFromGen2()
        self.activeVisit[visit] = Visit(visitId=visit, consumer=consumer, name=name)
        return self.activeVisit[visit]

    def gatherConsumers(self):
        """ Gather active visit consumers. """
        visit0Consumers = [] if not self.validVisit0 else self.visit0.activeConsumers()
        activeConsumers = [visit.consumer for visit in self.activeVisit.values()]
        return list(set(visit0Consumers + activeConsumers))

    def releaseVisit(self, visit=None, consumer=None):
        """ Release active visit. """
        if visit is None:
            try:
                [visit] = list(self.activeVisit.keys())
            except ValueError:
                raise RuntimeError(f'dont know which visit to release : {",".join(map(str, self.activeVisit.keys()))}')

        if self.validVisit0 and visit == self.visit0.visitId:
            self.visit0.releaseVisit(consumer)
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
    def __init__(self, visit):
        Visit.__init__(self, visit, consumer='fps')
        self.available = dict(fps=True, sps=True, agc=True)
        self.active = dict()
        self.visitSetBuffer = []

        self.setActive('fps')

    @property
    def visit0InsertedInOpDB(self):
        try:
            [visit0] = opDB.fetchone(f'select visit0 from pfs_config where visit0={self.visitId}')
        except:
            return False

        return True

    def stop(self):
        """ stop using that visit. """
        Visit.stop(self)
        for consumer in self.available.keys():
            self.available[consumer] = False

    def activeConsumers(self):
        """ Get list of active consumers. """
        return list(self.active.keys())

    def getVisit(self, consumer):
        """ check if visit0 is available for that given consumer"""
        if self.available[consumer]:
            self.setActive(consumer)
            return True

        return False

    def setActive(self, consumer):
        """ Set that consumer active. """
        self.available[consumer] = False
        self.active[consumer] = True

    def insertFieldSet(self, visit_set_id):
        """ add that visit_set_id to the buffer and try to insert. """
        self.visitSetBuffer.append(visit_set_id)
        self._insertFieldSet()

    def releaseVisit(self, consumer):
        """ release visit for that consumer. """
        self.active.pop(consumer, None)

    def _insertFieldSet(self):
        """ insert into field_set table. """
        if self.visit0InsertedInOpDB:
            while self.visitSetBuffer:
                visit_set_id = self.visitSetBuffer[0]
                opDB.insert('field_set', visit_set_id=visit_set_id, visit0=self.visitId)
                self.visitSetBuffer.remove(visit_set_id)

    def reset(self, cmd):
        """ last chance before resetting. """
        self._insertFieldSet()

        for visit_set_id in self.visitSetBuffer:
            cmd.warn(f'text="not inserted: '
                     f'SQL\INSERT INTO field_set (visit_set_id, visit0) VALUES ({visit_set_id}, {self.visitId})/SQL')
