__author__ = 'alefur'

import logging
import time
from importlib import reload

import ics.utils.sps.lamps.simulators.digitalLoggers as simulator
import ics.utils.sps.lamps.utils.lampState as lampUtils
import ics.utils.tcp.bufferedSocket as bufferedSocket
from ics.utils.fsm.fsmThread import FSMThread

reload(lampUtils)
reload(simulator)


class digitalLoggers(FSMThread, bufferedSocket.EthComm):
    # for state machine, not need to temporize before init
    forceInit = True

    def __init__(self, actor, name, loglevel=logging.DEBUG):
        """This sets up the connections to/from the hub, the logger, and the twisted reactor.

        :param actor: FsmActor.
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

        self.monitor = 0
        self.abortWarmup = False
        self.config = dict()
        self.outletConfig = dict()
        self.lampStates = dict()

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

    @property
    def simulated(self):
        """Return True if self.mode=='simulation', return False if self.mode='operation'."""
        if self.mode == 'simulation':
            return True
        elif self.mode == 'operation':
            return False
        else:
            raise ValueError('unknown mode')

    @property
    def lampsOn(self):
        return [lamp for lamp in self.lampNames if self.lampStates[lamp].lampOn]

    def _loadCfg(self, cmd, mode=None):
        """Load lamps configuration.

        :param cmd: current command.
        :param mode: operation|simulation, loaded from config file if None.
        :type mode: str
        :raise: Exception if config file is badly formatted.
        """
        self.mode = self.controllerConfig['mode'] if mode is None else mode
        self.lampNames = self.controllerConfig['lampNames']
        self.sim = simulator.Sim()

        bufferedSocket.EthComm.__init__(self,
                                        host=self.controllerConfig['host'],
                                        port=self.controllerConfig['port'],
                                        EOL='\r\n')

    def _openComm(self, cmd):
        """Open socket with lamps controller or simulate it.

        :param cmd: current command.
        :raise: socket.error if the communication has failed.
        """
        self.ioBuffer = bufferedSocket.BufferedSocket(self.name + 'IO', EOL='tcpover\n')
        s = self.connectSock()

    def _closeComm(self, cmd):
        """Close socket.

        :param cmd: current command.
        """
        self.closeSock()

    def _testComm(self, cmd):
        """Test communication.

        :param cmd: current command.
        :raise: Exception if the communication has failed with the controller.
        """
        self.sendOneCommand('getState', cmd=cmd)

    def _init(self, cmd):
        """Instanciate lampState for each lamp and switch them off by safety."""

        lampNames = self._getOutletsConfig(cmd)
        cmd.inform(f'lampNames={",".join(lampNames)}')
        cmd.inform(f'{self.name}pduModel=digitalLoggers')

        for lamp in lampNames:
            self.lampStates[lamp] = lampUtils.LampState(lamp)

        self.switchOff(cmd, lampNames)

    def getStatus(self, cmd):
        """Get all ports status.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        states = self.sendOneCommand('getState', cmd=cmd)
        self.genAllKeys(cmd, states)

    def genKeys(self, cmd, lampState, genTimeStamp=False):
        """ Generate one lamp keywords.

        :param cmd: current command.
        :param lampState: single lamp state
        :raise: Exception with warning message.
        """
        lamp, state = [r.strip() for r in lampState.split('=')]

        # if the outlet is actually a lamp, which is no longer a guarantee.
        if lamp in self.lampStates.keys():
            self.lampStates[lamp].setState(state, genTimeStamp=genTimeStamp)
            cmd.inform(f'{lamp}={str(self.lampStates[lamp])}')
        # crude outlet status otherwise.
        else:
            cmd.inform(f'{lamp}={state}')

    def genAllKeys(self, cmd, states, genTimeStamp=False):
        """ Generate all lamps keywords.

        :param cmd: current command.
        :param states: all lamp states
        :raise: Exception with warning message.
        """
        for lampState in states.split(','):
            self.genKeys(cmd, lampState, genTimeStamp=genTimeStamp)

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
        lampState : `str`
            returned string from socket IO.
        """
        cmd.debug(f'text="switching {desiredState} {outletName} now !"')
        lampState = self.sendOneCommand(f'switch {outletName} {desiredState}', cmd=cmd)
        return lampState

    def switchOff(self, cmd, lamps):
        """Switch off lamp list.

        :param cmd: current command.
        :param lamps: ['hgar', 'neon']
        :type lamps: list.
        :raise: Exception with warning message.
        """
        for lamp in lamps:
            lampState = self.crudeSwitch(cmd, lamp, 'off')
            self.genKeys(cmd, lampState, genTimeStamp=self.lampStates[lamp].lampOn)

    def prepare(self, cmd):
        """Configure a future illumination sequence.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        cmdStr = f'prepare {" ".join(sum([[lamp, str(time)] for lamp, time in self.config.items()], []))}'
        return self.sendOneCommand(cmdStr, cmd=cmd)

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

        for lamp in lamps:
            # no need to switch on.
            if lamp not in self.lampsOn:
                lampState = self.crudeSwitch(cmd, lamp, 'on')
                self.genKeys(cmd, lampState, genTimeStamp=True)

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
        timeout = max(self.config.values()) + 2
        timeLim = time.time() + timeout + 10

        # Dont close socket in that case.
        replies = bufferedSocket.EthComm.sendOneCommand(self, cmdStr='go', cmd=cmd).split('\n')
        states = replies[-1]

        for reply in replies[:len(replies) - 1]:
            cmd.inform(f'text="{reply}"')

        self.genAllKeys(cmd, states)

        reply = self.getOneResponse(cmd=cmd, timeout=timeout)

        while ';;' not in reply:
            if reply:
                self.genKeys(cmd, reply, genTimeStamp=True)

            reply = self.getOneResponse(cmd=cmd, timeout=timeout)
            if time.time() > timeLim:
                raise TimeoutError('lamps has not been triggered correctly')

        status, ret = reply.split(';;')

        if status != 'OK':
            raise RuntimeError(ret)

        self.genAllKeys(cmd, states)
        self._closeComm(cmd)

    def _getOutletsConfig(self, cmd):
        """Get all ports status.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        outlets = self.sendOneCommand('getOutletsConfig', cmd=cmd)

        for ret in outlets.split(','):
            cmd.inform(ret)
            outlet, lamp = [r.strip() for r in ret.split('=')]

            # additional check that the pdu config/actor config actually match
            if lamp not in self.lampNames:
                cmd.warn(f'text="{lamp} not listed in lampConfig, just consider it as an aside outlet..."')

            self.outletConfig[outlet] = lamp

        notConfigured = set(self.lampNames) - set(self.outletConfig.values())
        if notConfigured:
            raise ValueError(f'lamps : {",".join(notConfigured)} not described in pdu config')

        return self.lampNames

    def doAbort(self):
        """Abort warmup."""
        self.abortWarmup = True

        # if currently in the go sequence.
        if self.substates.current == 'TRIGGERING':
            # Send abort, but do not try to get any bytes from the server in this thread.
            # We are already getting output in a loop, eg, line 252.

            bufferedSocket.EthComm.sendAll(self, 'abort')

        # see ics.utils.fsm.fsmThread.LockedThread
        self.waitForCommandToFinish()
        self.abortWarmup = False

        return

    def sendOneCommand(self, cmdStr, doClose=False, cmd=None):
        """Send one command and return one response.

        :param cmdStr: string to send.
        :param doClose: If True, the device socket is closed before returning.
        :param cmd: current command.
        :return: reply : the single response string, with EOLs stripped.
        :raise: IOError : from any communication errors.
        """
        # The current lua tcp server is really simple and close the connection after a single command.
        # I'm not even mentioning threading here ....
        reply = bufferedSocket.EthComm.sendOneCommand(self, cmdStr=cmdStr, doClose=True, cmd=cmd)
        status, ret = reply.split(';;')

        if status != 'OK':
            raise RuntimeError(ret)

        return ret

    def createSock(self):
        """Create socket in operation, simulator otherwise.
        """
        if self.simulated:
            s = self.sim
        else:
            s = bufferedSocket.EthComm.createSock(self)

        return s

    def leaveCleanly(self, cmd):
        """Clear and leave.

        :param cmd: current command.
        """
        self.monitor = 0
        self.doAbort()

        try:
            self.switchOff(cmd, self.lampNames)
            self.getStatus(cmd)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

        self._closeComm(cmd=cmd)
