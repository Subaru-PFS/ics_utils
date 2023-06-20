__author__ = 'alefur'

import logging
import time

import ics.utils.sps.pdu.simulators.aten as atenSim
import ics.utils.tcp.bufferedSocket as bufferedSocket
from ics.utils.fsm.fsmThread import FSMThread


class aten(FSMThread, bufferedSocket.EthComm):
    # for state machine, not need to temporize before init
    forceInit = True

    maxIOAttempt = 3
    waitBetweenAttempt = 1
    socketTimeout = 1
    bufferTimeout = 1
    loginTimeout = 50

    def __init__(self, actor, name, loglevel=logging.DEBUG):
        """This sets up the connections to/from the hub, the logger, and the twisted reactor.

        :param actor: FsmActor.
        :param name: controller name.
        :type name: str
        """
        substates = ['IDLE', 'SWITCHING', 'FAILED']
        events = [{'name': 'switch', 'src': 'IDLE', 'dst': 'SWITCHING'},
                  {'name': 'idle', 'src': ['SWITCHING', ], 'dst': 'IDLE'},
                  {'name': 'fail', 'src': ['SWITCHING', ], 'dst': 'FAILED'},
                  ]

        FSMThread.__init__(self, actor, name, events=events, substates=substates)

        self.addStateCB('SWITCHING', self.switching)
        self.sim = atenSim.Sim()

        self.loginTime = 0

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
    def sessionExpired(self):
        return self.loginTime and (time.time() - self.loginTime) > self.loginTimeout

    def _loadCfg(self, cmd, name=None, mode=None):
        """Load pdu configuration.

        :param cmd: current command.
        :param mode: operation|simulation, loaded from config file if None.
        :type mode: str
        :raise: Exception if config file is badly formatted.
        """
        controllerConfig = self.controllerConfig if name is None else self.actor.actorConfig[name]
        self.mode = controllerConfig['mode'] if mode is None else mode
        bufferedSocket.EthComm.__init__(self,
                                        host=controllerConfig['host'],
                                        port=controllerConfig['port'],
                                        EOL='\r\n', stripTelnet=True)
        self.powerNames = dict([(str(key).zfill(2), val) for (key, val) in controllerConfig['outlets'].items()])
        self.powerPorts = dict([(val, key) for (key, val) in self.powerNames.items()])


        def loadOptionalConfig(option):
            """ Convenience to load optional config."""
            try:
                return self.controllerConfig[option]
            except KeyError:
                return getattr(self, option)

        self.maxIOAttempt = loadOptionalConfig('maxIOAttempt')
        self.waitBetweenAttempt = loadOptionalConfig('waitBetweenAttempt')
        self.socketTimeout = loadOptionalConfig('socketTimeout')
        self.bufferTimeout = loadOptionalConfig('bufferTimeout')
        self.loginTimeout = loadOptionalConfig('loginTimeout')

    def _openComm(self, cmd):
        """Open socket with pdu controller or simulate it.

        :param cmd: current command.
        :raise: socket.error if the communication has failed.
        """
        self.ioBuffer = bufferedSocket.BufferedSocket(self.name + 'IO', EOL='\r\n\r\n>', timeout=self.bufferTimeout)
        s = self.connectSock()

    def _closeComm(self, cmd):
        """Close socket.

        :param cmd: current command.
        """
        self.closeSock()
        self.loginTime = 0

    def _testComm(self, cmd):
        """Test communication.

        :param cmd: current command.
        :raise: Exception if the communication has failed with the controller.
        """
        v = float(self.safeOneCommand('read meter olt o01 volt simple', cmd=cmd))

    def getStatus(self, cmd):
        """Get all ports status.

        :param cmd: current command.
        :raise: Exception with warning message.
        """
        for outlet in self.powerNames.keys():
            self.portStatus(cmd, outlet=outlet)

    def portStatus(self, cmd, outlet):
        """Get state, voltage, current, power for a given outlet.

        :param cmd: current command.
        :param outlet: outlet number (ex : o01).
        :type outlet: str
        :raise: Exception with warning message.
        """
        state = self.safeOneCommand('read status o%s simple' % outlet, cmd=cmd)
        try:
            v = float(self.sendOneCommand('read meter olt o%s volt simple' % outlet, cmd=cmd))
        except:
            v = float('nan')
        try:
            a = float(self.sendOneCommand('read meter olt o%s curr simple' % outlet, cmd=cmd))
        except:
            a = float('nan')
        try:
            w = float(self.sendOneCommand('read meter olt o%s pow simple' % outlet, cmd=cmd))
        except:
            w = float('nan')

        cmd.inform('pduPort%d=%s,%s,%.2f,%.2f,%.2f' % (int(outlet), self.powerNames[outlet], state, v, a, w))

    def switching(self, cmd, powerPorts):
        """Switch on/off powerPorts dictionary.

        :param cmd: current command.
        :param powerPorts: dict(1=off, 2=on).
        :type powerPorts: dict.
        :raise: Exception with warning message.
        """
        for outlet, state in powerPorts.items():
            self.safeOneCommand('sw o%s %s imme' % (outlet, state), cmd=cmd)
            self.portStatus(cmd, outlet=outlet)

    def loginCommand(self, cmdStr, cmd=None, ioEOL=None):
        """Used to login.

        :param cmd: current command.
        :param cmdStr: string to send.
        :raise: Exception with warning message.
        """
        self.ioBuffer.EOL = ioEOL if ioEOL is not None else self.ioBuffer.EOL
        return bufferedSocket.EthComm.sendOneCommand(self, cmdStr=cmdStr, cmd=cmd)

    def safeOneCommand(self, cmdStr, doClose=False, cmd=None, nAttempt=0):
        """Used to login.

        :param cmd: current command.
        :param cmdStr: string to send.
        :raise: Exception with warning message.
        """
        if self.sessionExpired:
            cmd.debug('text="session might be expired, logging out now..."')
            self._closeComm(cmd)

        try:
            return self.sendOneCommand(cmdStr, doClose=doClose, cmd=cmd)
        except Exception as e:
            self._closeComm(cmd)
            if nAttempt < self.maxIOAttempt:
                cmd.warn('text=%s' % self.actor.strTraceback(e))
                cmd.warn(f'text="attempt #{nAttempt + 1} to fix connection')
                time.sleep(self.waitBetweenAttempt)
                return self.safeOneCommand(cmdStr, doClose=doClose, cmd=cmd, nAttempt=nAttempt + 1)
            raise

    def sendOneCommand(self, cmdStr, doClose=False, cmd=None):
        """Send one command and return one response.

        :param cmdStr: string to send.
        :param doClose: If True (the default), the device socket is closed before returning.
        :param cmd: current command.
        :return: reply : the single response string, with EOLs stripped.
        :raise: IOError : from any communication errors.
        """
        fullCmd = '%s%s' % (cmdStr, self.EOL)
        reply = bufferedSocket.EthComm.sendOneCommand(self, cmdStr=cmdStr, doClose=doClose, cmd=cmd)

        if not reply:
            raise IOError(f'no reply from ioBuffer(timeout={self.bufferTimeout}), socket might be broken...')

        if fullCmd not in reply:
            raise RuntimeError(f'Command({cmdStr}) was not echoed properly ret:{reply}')

        return reply.split(fullCmd)[1].strip()

    def connectSock(self):
        """Connect socket if self.sock is None.

        :param cmd: current command.
        """
        if self.sock is None:
            s = self.createSock()
            s.settimeout(self.socketTimeout)
            s.connect((self.host, self.port))
            self.sock = s
            self.authenticate()

        return self.sock

    def authenticate(self, pwd=None):
        """Log to the telnet server.

        :param pwd: password.
        """
        pwd = f'pdu.{self.actor.name}' if pwd is None else pwd
        try:
            self.loginCommand('teladmin', ioEOL='Password: ')
            self.loginCommand(pwd, ioEOL='Telnet server 1.1\r\n\r\n>')

            self.ioBuffer.EOL = '\r\n\r\n>'
            self.loginTime = time.time()
        except:
            self.sock = None
            raise

    def createSock(self):
        """Create socket in operation, simulator otherwise.
        """
        if self.simulated:
            s = self.sim
        else:
            s = bufferedSocket.EthComm.createSock(self)

        return s
