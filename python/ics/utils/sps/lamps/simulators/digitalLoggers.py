__author__ = 'alefur'

import operator
import socket
import time
from threading import Thread

import ics.utils.sps.lamps.utils.lampState as lampState


class Sim(socket.socket):
    allDcbLamps = list(set(lampState.allLamps) - {'hgcd'})
    lampNames = allDcbLamps + ['filterwheel']

    def __init__(self):
        """Fake lamps tcp server."""
        socket.socket.__init__(self, socket.AF_INET, socket.SOCK_STREAM)
        self.outlets = dict([(lamp, 'off') for lamp in Sim.lampNames])
        self.buf = []
        self.config = dict()
        self.doAbort = False

    def connect(self, server):
        """Fake the connection to tcp server."""
        (ip, port) = server
        time.sleep(0.2)
        if type(ip) is not str:
            raise TypeError
        if type(port) is not int:
            raise TypeError

    def sendall(self, cmdStr, flags=None):
        """Send fake packets, append fake response to buffer."""
        time.sleep(0.02)
        cmdStr, __ = cmdStr.decode().split('\r\n')

        if 'prepare' in cmdStr:
            lampsArgs = cmdStr.split(' ')[1:]
            self.config.clear()
            for i in range(int(len(lampsArgs) / 2)):
                self.config[lampsArgs[2 * i]] = float(lampsArgs[2 * i + 1])
            time.sleep(0.1)
            self.buf.append('OK;;OKtcpover\n')

        elif 'getState' in cmdStr:
            lampStates = self.getState()
            time.sleep(0.1)
            self.buf.append(f'OK;;{lampStates}tcpover\n')

        elif 'getOutletsConfig' in cmdStr:
            lampStates = self.getOutletsConfig()
            time.sleep(0.1)
            self.buf.append(f'OK;;{lampStates}tcpover\n')

        elif 'switch' in cmdStr:
            __, lamp, state = cmdStr.split(' ')
            self.outlets[lamp] = state
            time.sleep(0.1)
            lampState = f'{lamp}={state}'
            self.buf.append(f'OK;;{lampState}tcpover\n')

        elif 'go' in cmdStr:
            self.doAbort = False
            self.go()

        elif 'abort' in cmdStr:
            self.doAbort = True
            self.buf.append('tcpover\n')

    def go(self):

        for i, (lamp, secs) in enumerate(self.config.items()):
            self.buf.append(f'{lamp} {i} {secs}\n')

        longest, maxSecs = max(self.config.items(), key=operator.itemgetter(1))
        self.buf.append(f"{len(self.config)} channels active, longest {longest} {maxSecs} seconds\n")
        self.buf.append(f'{self.getState()}tcpover\n')

        lamps = []
        start = []
        stop = []
        for i, (lamp, secs) in enumerate(self.config.items()):
            start.append(time.time())
            stop.append(start[i] + secs)
            lamps.append(lamp)
            self.outlets[lamp] = 'on'
            self.buf.append(f'{lamp}=ontcpover\n')

        f1 = Thread(target=self.fireLamps, args=(lamps, stop, longest))
        f1.start()

    def fireLamps(self, lamps, stop, longest):
        while 1:
            for i in range(len(lamps)):
                lamp = lamps[i]
                if (self.outlets[lamp] == 'on' and time.time() > stop[i]) or self.doAbort:
                    self.outlets[lamp] = 'off'
                    self.buf.append(f'{lamp}=offtcpover\n')

            if self.outlets[longest] == 'off':
                break

        self.buf.append(f'OK;;{self.getState()}tcpover\n')

    def getState(self):
        return ','.join([f'{lamp}={state}' for lamp, state in self.outlets.items()])

    def getOutletsConfig(self):
        return ','.join([f'outlet0{i + 1}={lamp}' for i, lamp in enumerate(self.lampNames)])

    def recv(self, buffersize, flags=None):
        """Return and remove fake response from buffer."""
        time.sleep(0.02)
        try:
            ret = self.buf[0]
            self.buf = self.buf[1:]
            return str(ret).encode()
        except IndexError:
            raise IOError

    def close(self):
        pass
