#!/usr/bin/env python


import configparser
import logging

import actorcore.ICC
from ics.utils.fsm.FSM import MetaStates
from ics.utils.instdata import InstData
from twisted.internet import reactor


class FsmActor(actorcore.ICC.ICC):

    def __init__(self, name, productName=None, configFile=None, logLevel=logging.INFO, **kwargs):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.ICC.ICC.__init__(self, name,
                                   productName=productName,
                                   configFile=configFile,
                                   **kwargs)

        self.instData = InstData(self)
        self.logger.setLevel(logLevel)

        self.ignoreControllers = []
        self.metaStates = MetaStates(self)
        self.everConnected = False

    @property
    def monitors(self):
        """Return controller monitor value."""
        return dict([(name, controller.monitor) for name, controller in self.controllers.items()])

    def controllerKey(self):
        """Return formatted keyword listing all loaded controllers."""
        controllerNames = list(self.controllers.keys())
        key = f'controllers={",".join([c for c in controllerNames]) if controllerNames else None}'

        return key

    def reloadConfiguration(self, cmd):
        """Reload configuration file and generate keywords."""
        self.genInstConfigKeys(cmd)

    def genInstConfigKeys(self, cmd):
        """ Generate config keywords"""
        # leaving that here for now, not sure it will last long.
        cmd.inform('sections=%08x,"%r"' % (id(self.config), self.config))

    def letsGetReadyToRumble(self):
        """ just startup nicely"""
        pass

    def connectionMade(self):
        """Attach all controllers."""
        if self.everConnected is False:
            # reversing the starting logic, look safer to me.
            try:
                ignoreControllers = [s.strip() for s in self.config.get(self.name, 'ignoreControllers').split(',')]
            except configparser.NoOptionError:
                ignoreControllers = []

            self.ignoreControllers = ignoreControllers
            self.everConnected = True

            # keyword cannot be generated before the ping command kicks in, the actual startup is therefore delayed.
            reactor.callLater(5, self.letsGetReadyToRumble)

    def connect(self, controllerName, cmd=None, **kwargs):
        """Connect the given controller name.

        :param controller: controller name.
        :param cmd: current command.
        :type controller: str
        :raise: Exception with warning message.
        """
        cmd = self.bcast if cmd is None else cmd
        cmd.inform(f'text="attaching {controllerName}..."')
        try:
            self.attachController(controllerName, cmd=cmd, **kwargs)
        except:
            cmd.warn(self.controllerKey())
            cmd.warn(f'text="failed to connect controller {controllerName}"')
            raise

        cmd.inform(self.controllerKey())

    def disconnect(self, controllerName, cmd=None):
        """Disconnect the given controller name.

        :param controller: controller name.
        :param cmd: current command.
        :type controller: str
        :raise: Exception with warning message.
        """
        cmd = self.bcast if cmd is None else cmd
        cmd.inform(f'text="detaching {controllerName}..."')
        try:
            self.detachController(controllerName, cmd=cmd)

        except:
            cmd.warn(self.controllerKey())
            cmd.warn(f'text="failed to disconnect controller {controllerName}"')
            raise

        cmd.inform(self.controllerKey())

    def monitor(self, controller, period, cmd=None):
        """Change controller monitoring value.

        :param controller: controller name.
        :param period: monitoring value(secs).
        :param cmd: current command.
        :type controller: str
        :type period: int
        :raise: Exception with warning message.
        """
        cmd = self.bcast if cmd is None else cmd

        if controller not in self.controllers:
            raise ValueError(f'controller {controller} is not connected')

        self.controllers[controller].setMonitoring(period)
        cmd.warn(f'text="setting {controller} loop to {period}"')
