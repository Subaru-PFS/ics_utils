import ics.utils.visit.pfsField as pfsField
import ics.utils.visit.pfsVisit as pfsVisit
from pfscore.gen2 import fetchVisitFromGen2


class VisitManager(object):
    def __init__(self, actor):
        self.actor = actor
        self.activeField = self.reloadField()

        self.activeVisit = dict()  # do not think it's used anywhere but keeping it just in case.
        self.activePfsConfig = dict()

    def reloadField(self):
        """Reload persisted pfsField."""
        try:
            persisted = pfsField.PfsField.reload(self.actor)
        except:
            persisted = None

        return persisted

    def declareNewField(self, pfsDesignId, genVisit0=True):
        """Declare new field, read pfsDesign and get visit0."""
        visit0 = self._fetchVisitFromGen2(pfsDesignId=pfsDesignId) if genVisit0 else 0
        activeField = pfsField.PfsField.declareNew(self.actor, pfsDesignId, visit0)

        # if the same pfsDesign is re-declared for some reason hold on to the previous fpsConfig.
        if self.activeField and self.activeField.pfsDesignId == activeField.pfsDesignId:
            activeField.holdPfsConfig0(self.activeField.pfsConfig0)

        self.activeField = activeField

        return self.activeField.pfsDesign, self.activeField.visit0

    def getField(self):
        """Get active field."""
        if self.activeField is None:
            raise RuntimeError('no pfsDesign has been declared current...')

        return self.activeField

    def finishField(self):
        """Finish declaredCurrentPfsDesign."""
        self.activeField = None
        self.actor.actorData.persistKey('pfsField', None)

    def getCurrentDesignId(self):
        """Get declaredCurrentPfsDesignId."""
        return self.getField().pfsDesignId

    def getVisit(self, caller, name=None):
        """Get visit, visit0 if available otherwise new one."""
        if self.activeField:
            if not self.activeField.isVisitAvailableFor(caller):
                new = self.newVisit(caller, name=name)
                self.activeField.reconfigure(caller=caller, newVisit=new)

            return self.activeField.getVisit(caller)

        return self.newVisit(caller, name=name)

    def newVisit(self, caller, name=None):
        """Generate new visit."""
        visitId = self._fetchVisitFromGen2()
        return pfsVisit.Visit.fromCaller(visitId, caller, name=name)

    def _fetchVisitFromGen2(self, pfsDesignId=None):
        """Actually get a new visit from Gen2.
        What PFS calls a "visit", Gen2 calls a "frame".
        """
        return fetchVisitFromGen2(self.actor, designId=pfsDesignId)
