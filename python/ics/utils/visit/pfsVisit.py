import threading

import ics.utils.visit.exception as exception
from ics.utils.opdb import opDB


class Visit(object):
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
        self.lock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.unlock()

    @property
    def isPopulated(self):
        return opDB.fetchone(f'select pfs_visit_id from {self.exposureTable} where pfs_visit_id={self.visitId}')

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
            raise exception.VisitAlreadyDone()

        with self.idLock:
            frameIdx = self.__frameId
            if frameIdx >= 100:
                raise exception.VisitOverflowed()
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
        """Declare that we should not be used anymore."""
        self.iAmDead = True


class AgVisit(Visit):
    exposureTable = 'agc_exposure'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'ag', name=name)

    @property
    def isAvailable(self):
        return not self.isActive


class FpsVisit(Visit):
    exposureTable = 'mcs_exposure'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'fps', name=name)

    @property
    def isAvailable(self):
        return not self.isActive and not self.isPopulated


class SpsVisit(Visit):
    exposureTable = 'sps_visit'

    def __init__(self, visitId, name=None):
        Visit.__init__(self, visitId, 'sps', name=name)

    @property
    def isAvailable(self):
        """We actually always bump up sps after field acquisition."""
        return False
