__author__ = 'alefur'

import logging
import operator
import time
from datetime import datetime as dt
from importlib import reload

import ics.utils.sps.lamps.utils.lampState as lampUtils
import ics.utils.sps.pdu.simulators.aten as simulator
from ics.utils.fsm.fsmThread import FSMThread
from ics.utils.sps.pdu.controllers.aten import aten as atenPdu

reload(lampUtils)
reload(simulator)


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

        self.addStateCB('WARMING', self._doWarmup)
        self.addStateCB('TRIGGERING', self._doGo)
        self.sim = simulator.Sim()

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
        self.lampNames = self.controllerConfig['lampNames']

        for lamp in self.lampNames:
            # additional check that the pdu config/actor config actually match
            if lamp not in self.powerPorts.keys():
                raise ValueError(f'unknown lamp {lamp}, lampNames={",".join(self.lampNames)}')

        cmd.inform(f'lampNames={",".join(self.lampNames)}')
        cmd.inform(f'{self.name}pduModel=aten')

    def _init(self, cmd):
        """Instanciate lampState for each lamp and switch them off by safety."""

        for lamp in self.lampNames:
            self.lampStates[lamp] = lampUtils.LampState(lamp)

        self.getStatus(cmd)
        self.switchOff(cmd, self.lampsOn)

    def getStatus(self, cmd):
        """Get and generate lamps keywords.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        # we are actually iterating on all outlets now.
        for lamp in self.powerNames.values():
            state = self._getState(lamp, cmd=cmd)
            self.genKeys(cmd, lamp, state)

    def genKeys(self, cmd, lamp, state, genTimeStamp=False):
        """ Generate one lamp keywords.

        :param cmd: current command.
        :param lampState: single lamp state
        :raise: Exception with warning message.
        """
        # if the outlet is actually a lamp, which is no longer a guarantee.
        if lamp in self.lampStates.keys():
            self.lampStates[lamp].setState(state, genTimeStamp=genTimeStamp)
            cmd.inform(f'{lamp}={str(self.lampStates[lamp])}')
        # crude outlet status otherwise.
        else:
            cmd.inform(f'{lamp}={state}')

    def crudeSwitch(self, cmd, outletName, desiredState):
        """Crude  outlet switch on/off.

        Parameters
        ----------
        cmd :`actorcore.Command.Command`
            on-going mhs command.
        outletName : `str`
            the outlet name, very likely lamp name.
        desiredState : `str`
            the outlet desired state (off|on).

        Returns
        -------
        ret : `str`
            returned string from socket IO.
        """
        outletNumber = self.powerPorts[outletName]
        outletStr = f'o{outletNumber}'

        cmd.debug(f'text="switching {desiredState} outlet{outletNumber}:{outletName} now !"')
        return self.safeOneCommand(f'sw {outletStr} {desiredState} imme', cmd=cmd)

    def switchOff(self, cmd, lamps):
        """Switch off lamps

        :param cmd: current command.
        :param lamps: lamps to switch off.
        :type lamps: list
        :raise: Exception with warning message.
        """
        for lamp in lamps:
            self._switchOneOff(cmd, lamp)

        self._spinAllUntil(cmd, lamps, 'off')
        cmd.debug(f'text="outlets for {lamps} are now effectively turned off..."')

    def prepare(self, cmd):
        """Configure a future illumination sequence.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        cmd.inform('text="actually nothing to do..."')

    def _doWarmup(self, cmd, lamps, warmingTime=None):
        """warm up lamps list

        :param cmd: current command.
        :param lamps: ['hgar', 'neon']
        :type lamps: list.
        :raise: Exception with warning message.
        """

        def waitUntil(end, ti=0.01):
            """ Wait until time.time() >end.

            :param end: nb of secs since epoch.
            """
            while time.time() < end:
                time.sleep(ti)
                self.handleTimeout()
                if self.abortWarmup:
                    raise UserWarning('sources warmup aborted')

        self._switchOn(cmd, lamps)

        toBeWarmed = lamps if lamps else self.lampsOn
        if warmingTime is None:
            warmingTimes = [lampUtils.warmingTime[lamp] for lamp in toBeWarmed]
        else:
            warmingTimes = len(toBeWarmed) * [warmingTime]

        remainingTimes = [t - self.lampStates[lamp].elapsed() for t, lamp in zip(warmingTimes, toBeWarmed)]
        sleepTime = max(remainingTimes) if remainingTimes else 0

        if sleepTime > 0:
            cmd.inform(f'text="warmingTime:{max(warmingTimes)} now sleeping for {round(sleepTime)} secs'"")
            waitUntil(time.time() + sleepTime)

    def _doGo(self, cmd):
        """Run the preconfigured illumination sequence.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        # make sure no lamps are turned on in the first place.
        self.switchOff(cmd, self.lampsOn)

        lamp, maxSeconds = max(self.config.items(), key=operator.itemgetter(1))
        cmd.inform(f'text="{len(self.config)} channels active, longest {lamp} {maxSeconds} seconds"')

        lampNames = list(self.config.keys())
        self._switchOn(cmd, lampNames)

        switchOff = [(lamp, self.lampStates[lamp].switchOffTiming(seconds)) for lamp, seconds in self.config.items()]
        switchOff.sort(key=lambda tup: tup[1])

        for lamp, offTiming in switchOff:
            while dt.utcnow() < offTiming:
                time.sleep(0.01)
                if self.abortWarmup:
                    self.switchOff(cmd, self.lampsOn)
                    raise UserWarning('sources warmup aborted')

            self._switchOneOff(cmd, lamp)

        self._spinAllUntil(cmd, lampNames, 'off')

    def _getState(self, lamp, cmd):
        """Get current light source state.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        return self.safeOneCommand('read status o%s simple' % self.powerPorts[lamp], cmd=cmd)

    def _switchOn(self, cmd, lamps):
        """ Switch all given lamps on and spin until pdu declares outlets are on.

        Parameters
        ----------
        cmd :`actorcore.Command.Command`
            on-going mhs command.
        lampsOn : `list` of `str`
            list of lamp to switch on.
        """
        # dont switch lamp which are already on.
        toSwitchOn = [lamp for lamp in lamps if lamp not in self.lampsOn]

        # switch all lamps on first.
        for lamp in toSwitchOn:
            self.crudeSwitch(cmd, lamp, 'on')

        # spin until the pdu declares all lamps to be on.
        try:
            self._spinAllUntil(cmd, toSwitchOn, 'on')
        except:
            # something wrong happen so turn everything off.
            try:
                self.switchOff(cmd, toSwitchOn)
                switchedOff = True
            except:
                switchedOff = False

            raise RuntimeError(f"switch lamp {','.join(toSwitchOn)} did not turn on! "
                               f"all ports switched back off: {switchedOff}")

    def _switchOneOff(self, cmd, lamp):
        """Switch one lamp off

        :param cmd: current command.
        :param lamps: lamps to switch off.
        :type lamps: list
        :raise: Exception with warning message.
        """
        self.crudeSwitch(cmd, lamp, 'off')
        self.genKeys(cmd, lamp, 'off', genTimeStamp=True)

    def _spinAllUntil(self, cmd, lamps, desiredState, timeout=5):
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
                    state = self._getState(lamp, cmd=cmd)
                    cmd.debug(f'text="{lamp}={state}"')
                    if state == desiredState:
                        if state == 'on':
                            self.genKeys(cmd, lamp, state, genTimeStamp=True)

                        pending.remove(lamp)

            if t1 - t0 > timeout:
                raise RuntimeError(f"FAILED to switch {','.join(lamps)} to {desiredState} within {t1 - t0} seconds")

            time.sleep(0.05)

    def authenticate(self, pwd='pfsait'):
        """Log to the telnet server.

        :param pwd: password.
        """
        atenPdu.authenticate(self, pwd=pwd)

    def doAbort(self):
        """Abort warmup."""
        self.abortWarmup = True

        # see ics.utils.fsm.fsmThread.LockedThread
        self.waitForCommandToFinish()
        self.abortWarmup = False

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
