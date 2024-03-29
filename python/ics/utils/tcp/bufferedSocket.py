import logging
import select
import socket


class EthComm(object):
    def __init__(self, host, port, EOL='\r\n', stripTelnet=False):
        object.__init__(self)
        self.sock = None
        self.host = host
        self.port = port
        self.EOL = EOL
        self.stripTelnet = stripTelnet

        try:
            self.logger.debug(f'instanciating EthComm {host}:{port}')
        except AttributeError:
            self.logger = logging.getLogger(f'{host}:{port}')
            self.logger.setLevel(logging.DEBUG)

    def createSock(self):
        """Create regular socket object."""
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connectSock(self, timeout=3.0):
        """Connect socket if self.sock is None."""
        if self.sock is None:
            s = self.createSock()
            s.settimeout(timeout)
            s.connect((self.host, self.port))

            self.sock = s

        return self.sock

    def closeSock(self):
        """Close the socket and delete object."""
        try:
            self.sock.close()
        except:
            pass

        self.sock = None

    def sendOneCommand(self, cmdStr, doClose=False, cmd=None):
        """Send one command and return one response.

        Parameters
        ----------
        cmdStr : str
            The command to send.
        doClose : bool
            If True (the default), the device socket is closed before returning.
        cmd :
            ongoing command.

        Returns
        -------
        reply : str
            the single response string, with EOLs stripped.
        """
        cmd = self.actor.bcast if cmd is None else cmd

        # add EOL to cmdStr and convert to byte
        sock = self.sendAll(cmdStr)
        # get a EOL stripped response as str.
        reply = self.getOneResponse(sock=sock, cmd=cmd)

        if doClose:
            self.closeSock()

        return reply

    def sendAll(self, cmdStr):
        """Attempt to send data to the socket.

        Parameters
        ----------
        cmdStr : str
            The command to send.

        Returns
        -------
        sock : socket.socket
            the opened socket.
        """
        fullCmd = ('%s%s' % (cmdStr, self.EOL)).encode('latin-1')
        self.logger.debug('sending %r', fullCmd)

        sock = self.connectSock()

        try:
            sock.sendall(fullCmd)
        except Exception as e:
            self.logger.warning("%s failed to send '%s': %s" % (self.name, fullCmd, e))
            self.closeSock()
            raise

        return sock

    def getOneResponse(self, sock=None, cmd=None, timeout=None):
        """Attempt to receive data from the socket.

        Parameters
        ----------
        sock : socket.socket
            current socket.
        cmd : str
            ongoing command.
        timeout : int
            timeout value.

        Returns
        -------
        reply : str
            stripped reply as string.
        """
        if sock is None:
            sock = self.connectSock()

        ret = self.ioBuffer.getOneResponse(sock=sock, cmd=cmd, timeout=timeout)

        if self.stripTelnet:
            self.logger.debug('raw received %r', ret)
            ret = self._stripTelnet(ret)

        reply = ret.strip()
        if reply:
            self.logger.debug('received %r', reply)

        return reply

    def _stripTelnet(self, s):
        """Crudely strip TELNET negotiation goo from our input.

        Caveats:
         - Does not reply to DO with WONT or to WILL with DONT
         - Only reports to .logger.
         - Accepts codes which we do not understand, and always strips those out
           as if they are single byte codes. May or may not be true.

        Parameters
        ----------
        s : str
            input string.
        Returns
        -------
        reply : str
            stripped string.
        """
        IAC = chr(255)
        NOP = 241
        WILL = 251
        WONT = 252
        DO = 253
        DONT = 254

        while True:
            start = s.find(IAC)
            if start == -1:
                return s
            s1 = s[:start]
            cmd = ord(s[start + 1])
            if cmd in {WILL, WONT, DO, DONT}:
                cmd2 = ord(s[start + 2])
                s2 = s[start + 3:]
            elif cmd in {NOP}:
                cmd2 = 'OK'
                s2 = s[start + 2:]
            else:
                cmd2 = 'UNKNOWN!'
                s2 = s[start + 2:]
            self.logger.debug(f'stripping {cmd}.{cmd2}')
            s = s1 + s2


class BufferedSocket(object):
    """ Buffer the input from a socket and block it into lines. """

    def __init__(self, name, sock=None, loggerName=None, EOL='\n', timeout=3.0, logLevel=logging.INFO):
        self.EOL = EOL
        self.sock = sock
        self.name = name
        self.logger = logging.getLogger(loggerName)
        self.logger.setLevel(logLevel)
        self.timeout = timeout

        self.buffer = ''

    def getOutput(self, sock=None, timeout=None, cmd=None):
        """Block/timeout for input, then return all (<=1kB) available input."""
        if sock is None:
            sock = self.sock

        if timeout is None:
            timeout = self.timeout

        readers, writers, broken = select.select([sock.fileno()], [], [], timeout)
        if len(readers) == 0:
            msg = "%s: timed out (%s s) reading input from %s" % (self.name, timeout, self.name)
            self.logger.warn(msg)
            cmd.warn('text="%s"' % (msg))
            raise IOError(msg)

        return sock.recv(1024).decode('latin-1')

    def getOneResponse(self, sock=None, timeout=None, cmd=None, doRaise=False):
        """Return the next available complete line. Fetch new input if necessary.

        Parameters
        ----------
        sock : socket
           Uses self.sock if not set.
        timeout : float
           Uses self.timeout if not set.

        Returns
        -------
        ret : str
            a single line of response text, with EOL character(s) stripped.
        """
        while self.buffer.find(self.EOL) == -1:
            try:
                more = self.getOutput(sock=sock, timeout=timeout, cmd=cmd)
                if not more:
                    if doRaise:
                        raise IOError("getOneResponse received nothing.")
                    else:
                        return self.getOneResponse(sock=sock, timeout=timeout, cmd=cmd, doRaise=True)

            except IOError:
                return ''

            self.logger.debug('%s added: %r' % (self.name, more))
            self.buffer += more

        eolAt = self.buffer.find(self.EOL)
        ret = self.buffer[:eolAt]

        self.buffer = self.buffer[eolAt + len(self.EOL):]

        return ret
