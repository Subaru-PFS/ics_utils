#!/usr/bin/env python


import ics.utils.tcp.utils as tcpUtils
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from ics.utils.threading import threaded, singleShot


class PduCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('pdu', 'status', self.status),
            ('power', 'status', self.status),
            ('power', '[<on>] [<off>]', self.switch),
            ('pdu', 'stop', self.stop),
            ('pdu', 'start [@(operation|simulation)]', self.start),

        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("pdu__pdu", (1, 1),
                                        keys.Key("on", types.String() * (1, None),
                                                 help='which outlet to switch on.'),
                                        keys.Key("off", types.String() * (1, None),
                                                 help='which outlet to switch off.'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers['pdu']
        except KeyError:
            raise RuntimeError('pdu controller is not connected.')

    @threaded
    def status(self, cmd):
        """Report state, mode, status."""
        self.controller.generate(cmd)

    @threaded
    def switch(self, cmd):
        """Switch on/off outlets."""
        cmdKeys = cmd.cmd.keywords

        switchOn = cmdKeys['on'].values if 'on' in cmdKeys else []
        switchOff = cmdKeys['off'].values if 'off' in cmdKeys else []
        powerNames = dict([(name, 'on') for name in switchOn] + [(name, 'off') for name in switchOff])

        for name in powerNames.keys():
            if name not in self.controller.powerPorts.keys():
                raise ValueError('%s : unknown port' % name)

        powerPorts = dict([(self.controller.powerPorts[name], state) for name, state in powerNames.items()])

        self.controller.substates.switch(cmd, powerPorts=powerPorts)
        self.controller.generate(cmd)

    @singleShot
    def stop(self, cmd):
        """Disconnect controller."""
        self.actor.disconnect('pdu', cmd=cmd)
        cmd.finish()

    @singleShot
    def start(self, cmd):
        """Connect pdu controller."""
        cmdKeys = cmd.cmd.keywords
        mode = self.actor.config.get('pdu', 'mode')
        host = self.actor.config.get('pdu', 'host')
        port = self.actor.config.get('pdu', 'port')
        mode = 'operation' if 'operation' in cmdKeys else mode
        mode = 'simulation' if 'simulation' in cmdKeys else mode

        if mode == 'operation':
            # no need to do that in simulation.
            tcpUtils.waitForTcpServer(host=host, port=port, cmd=cmd)

        self.actor.connect('pdu', cmd=cmd, mode=mode)
        self.controller.generate(cmd)
