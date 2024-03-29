import ics.utils.time as pfsTime
from actorcore.QThread import QThread
from ics.utils.fsm.FSM import FSMDevice


class LockedThread(QThread):
    timeLimToFinish = 10
    """ There is no safe way to know whether a QThread is actually doing something.
        Its either waiting from queue.get(timeout=..) or calling a function. this class is basically a workaround. """

    def __init__(self, *args, **kwargs):
        QThread.__init__(self, *args, **kwargs)
        self.onGoingCmd = False

    @property
    def isAvailable(self):
        return self.onGoingCmd is False

    @property
    def isLocked(self):
        return not self.isAvailable

    def lock(self, cmd):
        self.onGoingCmd = cmd

    def unlock(self):
        self.onGoingCmd = False

    def waitForCommandToFinish(self, timeLim=None):
        timeLim = LockedThread.timeLimToFinish if timeLim is None else timeLim

        start = pfsTime.timestamp()
        while self.isLocked:
            if pfsTime.timestamp() - start > timeLim:
                raise RuntimeError(f'{str(self.onGoingCmd)} did not finished after {timeLim} secs !!!')

            # more important that you think...
            pfsTime.sleep.millisec()


class FSMThread(FSMDevice, LockedThread):
    # monitoring config
    minMonitorPeriod = 2
    defaultMonitorPeriod = 60

    def __init__(self, actor, name, events=False, substates=False):
        """This combine QThread and FSMDevice.

        :param actor: enuActor.
        :param name: controller name.
        :param events: event list for FSM device.
        :param substates: substates list for FSM device.
        """
        self.last = 0
        self.monitor = 60

        LockedThread.__init__(self, actor, name, timeout=30)
        FSMDevice.__init__(self, actor, name, events=events, substates=substates)

        self.setMonitoring(FSMThread.defaultMonitorPeriod)

    @property
    def controllerConfig(self):
        return self.actor.actorConfig[self.name]

    def loadCfg(self, cmd, mode=None):
        """Called by FSM loading state callback, loadCfg=>openComm=>testComm.

        :param cmd: current command.
        :param mode: operation|simulation, loaded from config file if None.
        :type mode: str
        :raise: Exception if config file is badly formatted.
        """
        self._loadCfg(cmd, mode=mode)
        FSMDevice.loadCfg(self, cmd)

    def openComm(self, cmd):
        """Called by FSM loading state callback, loadCfg=>openComm=>testComm.

        :param cmd: current command.
        :raise: socket.error if the communication has failed.
        """
        self._openComm(cmd)
        FSMDevice.openComm(self, cmd)

    def testComm(self, cmd):
        """Called by FSM loading state callback, loadCfg=>openComm=>testComm.

        :param cmd: current command.
        :raise: Exception if the communication has failed with the controller.
        """
        self._testComm(cmd)
        FSMDevice.testComm(self, cmd)

    def init(self, cmd, **kwargs):
        """Called by FSM initialising state callback.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        self._init(cmd, **kwargs)
        FSMDevice.init(self, cmd)

    def _loadCfg(self, cmd, mode=None):
        """Intended to be overridden."""
        pass

    def _openComm(self, cmd):
        """Intended to be overridden."""
        pass

    def _testComm(self, cmd):
        """Intended to be overridden."""
        pass

    def _closeComm(self, cmd):
        """Intended to be overridden."""
        pass

    def _init(self, cmd, **kwargs):
        """Intended to be overridden."""
        pass

    def leaveCleanly(self, cmd):
        """stop monitoring and close communication.

        :param cmd: current command.
        """
        self.monitor = 0
        self._closeComm(cmd=cmd)

    def start(self, cmd=None, **kwargs):
        """Start state machine and start QThread.

        :param cmd: current command.
        """
        try:
            FSMDevice.start(self, cmd=cmd, **kwargs)
            self.generate(cmd=cmd, doFinish=False)
        finally:
            QThread.start(self)

    def stop(self, cmd):
        """Free up any hardware ressources, stop state machine and stop QThread.

        :param cmd: current command.
        """
        self.leaveCleanly(cmd=cmd)
        FSMDevice.stop(self, cmd=cmd)
        self.exit()

    def generate(self, cmd=None, doFinish=True):
        """Generate FSM state and substate, current operating mode and get statuses.

        :param cmd: current command.
        :param doFinish: if True finish current command.
        """
        cmd = self.actor.bcast if cmd is None else cmd

        cmd.inform('%sFSM=%s,%s' % (self.name, self.states.current, self.substates.current))
        cmd.inform('%sMode=%s' % (self.name, self.mode))

        if self.states.current in ['LOADED', 'ONLINE']:
            try:
                self.getStatus(cmd)
            finally:
                # just generate keyword at constant rate, no matter if it fails or not.
                self.last = pfsTime.timestamp()
                self._closeComm(cmd)

        if doFinish:
            cmd.finish()

    def setMonitoring(self, period):
        """Set controller monitoring period.

        Parameters
        ----------
        period : `int`
           Monitoring period (seconds).
        """

        def getThreadTimeout(period):
            # set thread timeout to half the period, or 60 seconds if monitoring is deactivated.
            if not period:
                return FSMThread.defaultMonitorPeriod
            # return greatest divisor.
            for thp in range(2, period + 1):
                if period % thp == 0:
                    return period / thp

        if period and period < FSMThread.minMonitorPeriod:
            raise ValueError(f'minimum monitoring period is set to {FSMThread.minMonitorPeriod} seconds ...')

        self.timeout = getThreadTimeout(period)
        self.monitor = period

    def handleTimeout(self, cmd=None):
        """Called when thread is idle, generate keywords if monitor>0.

        :param cmd: current command.
        """
        cmd = self.actor.bcast if cmd is None else cmd

        if self.exitASAP:
            raise SystemExit()

        if self.monitor and (pfsTime.timestamp() - self.last) > self.monitor:
            try:
                self.generate(cmd)
            except Exception as e:
                cmd.fail('text=%s' % self.actor.strTraceback(e))
