__author__ = 'alefur'

import logging
import operator
import time
from datetime import datetime as dt
from importlib import reload

import ics.utils.sps.lamps.utils.lampState as lampUtils
import ics.utils.sps.pdu.simulators.aten as atenSim
from ics.utils.fsm.fsmThread import FSMThread
from ics.utils.sps.pdu.controllers.aten import aten as atenPdu

reload(lampUtils)


class aten(atenPdu):
    # for state machine, not need to temporize before init
    forceInit = True

    bufferTimeout = 3
    socketTimeout = 3

    def __init__(self, actor, name, loglevel=logging.DEBUG):
        """This sets up the connections to/from the hub, the logger, and the twisted reactor.

        :param actor: enuActor.
        :param name: controller name.
        :type name: str
        """
        substates = ['IDLE', 'WARMING', 'TRIGGERING', 'FAILED']
        events = [{'name': 'warming', 'src': 'IDLE', 'dst': 'WARMING'},
                  {'name': 'triggering', 'src': 'IDLE', 'dst': 'TRIGGERING'},
                  {'name': 'idle', 'src': ['WARMING', 'TRIGGERING', ], 'dst': 'IDLE'},
                  {'name': 'fail', 'src': ['WARMING', 'TRIGGERING', ], 'dst': 'FAILED'},
                  ]

        FSMThread.__init__(self, actor, name, events=events, substates=substates)

        self.addStateCB('WARMING', self.warmup)
        self.addStateCB('TRIGGERING', self.doGo)
        self.sim = atenSim.Sim()

        self.loginTime = 0
        self.abortWarmup = False
        self.config = dict()
        self.lampStates = dict()

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

    @property
    def lampsOn(self):
        return [lamp for lamp in self.lampNames if self.lampStates[lamp].lampOn]

    def _loadCfg(self, cmd, mode=None):
        """Load iis configuration.

        :param cmd: current command.
        :param mode: operation|simulation, loaded from config file if None.
        :type mode: str
        :raise: Exception if config file is badly formatted.
        """
        atenPdu._loadCfg(self, cmd, mode=mode)
        self.lampNames = [l.strip() for l in self.actor.config.get(self.name, 'lampNames').split(',')]

        for lamp in self.lampNames:
            # additional check that the pdu config/actor config actually match
            if lamp not in self.powerPorts.keys():
                raise ValueError(f'unknown lamp {lamp}, lampNames={",".join(self.lampNames)}')

        cmd.inform(f'lampNames={",".join(self.lampNames)}')
        cmd.inform(f'{self.name}pduModel=aten')

    def _init(self, cmd):
        """Instanciate lampState for each lamp and switch them off by safety."""

        for lamp in self.lampNames:
            self.lampStates[lamp] = lampUtils.LampState()

        self.getStatus(cmd)
        self.switchOff(cmd, self.lampsOn)

    def getStatus(self, cmd, lampNames=None):
        """Get and generate iis keywords.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        lampNames = self.lampNames if lampNames is None else lampNames

        for lamp in lampNames:
            state = self.getState(lamp, cmd=cmd)
            self.genKeys(cmd, lamp, state)

    def getState(self, lamp, cmd):
        """Get current light source state.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        return self.safeOneCommand('read status o%s simple' % self.powerPorts[lamp], cmd=cmd)

    def genKeys(self, cmd, lamp, state, genTimeStamp=False):
        """ Generate one lamp keywords.

        :param cmd: current command.
        :param lampState: single lamp state
        :raise: Exception with warning message.
        """
        self.lampStates[lamp].setState(state, genTimeStamp=genTimeStamp)
        cmd.inform(f'{lamp}={str(self.lampStates[lamp])}')

    def spinAllUntil(self, cmd, lamps, desiredState, timeout=5):
        """Get and generate iis keywords.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        t0 = time.time()
        pending = [lamp for lamp in lamps]
        cmd.debug(f'text="checking on outlet for {",".join(pending)}"')

        while pending:
            t1 = time.time()
            for lamp in lamps:
                if lamp in pending:
                    state = self.getState(lamp, cmd=cmd)
                    cmd.debug(f'text="{lamp}={state}"')
                    if state == desiredState:
                        if state == 'on':
                            self.genKeys(cmd, lamp, state, genTimeStamp=True)

                        pending.remove(lamp)

            if t1 - t0 > timeout:
                raise RuntimeError(f"FAILED to switch {','.join(lamps)} to {desiredState} within {t1 - t0} seconds")

            time.sleep(0.05)

    def switchOn(self, cmd, lamps):
        """Switch on lamps

        :param cmd: current command.
        :param lamps: lamps to switch off.
        :type lamps: list
        :raise: Exception with warning message.
        """
        toSwitchOn = [lamp for lamp in lamps if lamp not in self.lampsOn]
        for lamp in toSwitchOn:
            outlet = self.powerPorts[lamp]
            self.safeOneCommand('sw o%s on imme' % outlet, cmd=cmd)
        try:
            self.spinAllUntil(cmd, toSwitchOn, 'on')
        except:
            try:
                self.switchOff(cmd, lamps)
                switchedOff = True
            except:
                switchedOff = False

            raise RuntimeError(f"switch lamp {','.join(toSwitchOn)} did not turn on! "
                               f"all ports switched back off: {switchedOff}")

    def switchOneOff(self, cmd, lamp):
        """Switch one lamp off

        :param cmd: current command.
        :param lamps: lamps to switch off.
        :type lamps: list
        :raise: Exception with warning message.
        """
        outlet = self.powerPorts[lamp]
        cmd.debug(f'text="switching off outlet {outlet}:{lamp} now !"')

        self.safeOneCommand('sw o%s off imme' % outlet, cmd=cmd)
        self.genKeys(cmd, lamp, 'off', genTimeStamp=True)

    def switchOff(self, cmd, lamps):
        """Switch off lamps

        :param cmd: current command.
        :param lamps: lamps to switch off.
        :type lamps: list
        :raise: Exception with warning message.
        """
        for lamp in lamps:
            self.switchOneOff(cmd, lamp)

        self.spinAllUntil(cmd, lamps, 'off')
        cmd.debug(f'text="outlets for {lamps} are now effectively turned off..."')

    def warmup(self, cmd, lamps, warmingTime=None):
        """warm up lamps list

        :param cmd: current command.
        :param lamps: ['hgar', 'neon']
        :type lamps: list.
        :raise: Exception with warning message.
        """
        self.abortWarmup = False
        self.switchOn(cmd, lamps)

        toBeWarmed = lamps if lamps else self.lampsOn
        if warmingTime is None:
            warmingTimes = [lampUtils.warmingTime[lamp] for lamp in toBeWarmed]
        else:
            warmingTimes = len(toBeWarmed) * [warmingTime]

        remainingTimes = [t - self.lampStates[lamp].elapsed() for t, lamp in zip(warmingTimes, toBeWarmed)]
        sleepTime = max(remainingTimes) if remainingTimes else 0

        if sleepTime > 0:
            cmd.inform(f'text="warmingTime:{max(warmingTimes)} now sleeping for {round(sleepTime)} secs'"")
            self.wait(time.time() + sleepTime)

    def prepare(self, cmd):
        """Configure a future illumination sequence.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        cmd.inform('text="actually nothing to do..."')

    def doGo(self, cmd):
        """Run the preconfigured illumination sequence.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        self.abortWarmup = False
        lamp, maxSeconds = max(self.config.items(), key=operator.itemgetter(1))
        cmd.inform(f'text="{len(self.config)} channels active, longest {lamp} {maxSeconds} seconds"')

        lampNames = list(self.config.keys())
        self.switchOn(cmd, lampNames)

        switchOff = [(lamp, self.lampStates[lamp].switchOffTiming(seconds)) for lamp, seconds in self.config.items()]
        switchOff.sort(key=lambda tup: tup[1])

        for lamp, offTiming in switchOff:
            while dt.utcnow() < offTiming:
                time.sleep(0.01)
                if self.abortWarmup:
                    self.switchOff(cmd, self.lampsOn)
                    raise UserWarning('sources warmup aborted')

            self.switchOneOff(cmd, lamp)

        self.spinAllUntil(cmd, lampNames, 'off')

    def authenticate(self, pwd='pfsait'):
        """Log to the telnet server.

        :param pwd: password.
        """
        atenPdu.authenticate(self, pwd=pwd)

    def wait(self, end, ti=0.01):
        """ Wait until time.time() >end.

        :param end: nb of secs since epoch.
        """
        while time.time() < end:
            time.sleep(ti)
            self.handleTimeout()
            if self.abortWarmup:
                raise UserWarning('sources warmup aborted')

    def doAbort(self):
        """Abort warmup."""
        self.abortWarmup = True

        # see ics.utils.fsm.fsmThread.LockedThread
        self.waitForCommandToFinish()

        return

    def leaveCleanly(self, cmd):
        """Clear and leave.

        :param cmd: current command.
        """
        self.monitor = 0
        self.doAbort()

        try:
            self.switchOff(cmd, self.lampsOn)
            self.getStatus(cmd)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

        self._closeComm(cmd=cmd)
