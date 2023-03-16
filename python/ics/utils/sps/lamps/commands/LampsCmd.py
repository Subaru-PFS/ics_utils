#!/usr/bin/env python

import time

import ics.utils.sps.lamps.utils.lampState as lampState
import ics.utils.tcp.utils as tcpUtils
import ics.utils.time as pfsTime
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from ics.utils.threading import threaded, singleShot, blocking


class LampsCmd(object):
    def __init__(self, actor, name='lamps'):
        # This lets us access the rest of the actor.
        self.actor = actor
        self.name = name
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #

        self.vocab = [
            (name, 'status', self.status),
            (name, '[<on>] [<warmingTime>] [force]', self.warmup),
            (name, '<off>', self.switchOff),
            (name, 'stop', self.stop),
            (name, 'start [@(operation|simulation)]', self.start),

            ('prepare', '[<halogen>] [<argon>] [<neon>] [<krypton>] [<xenon>] [<hgar>] [<hgcd>]', self.prepare),
            ('go', '[<delay>] [@noWait]', self.go),
            ('stop', '', self.abort),
            ('abort', '', self.abort),
            ('waitForReadySignal', '', self.waitForReadySignal),
        ]

        self.vocab += [('sources', cmdStr, func) for __, cmdStr, func in self.vocab]
        self.vocab += [('arc', cmdStr, func) for __, cmdStr, func in self.vocab]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("lamps__lamps", (1, 1),
                                        keys.Key("on", types.String() * (1, None),
                                                 help='which outlet to switch on.'),
                                        keys.Key("off", types.String() * (1, None),
                                                 help='which outlet to switch off.'),
                                        keys.Key("warmingTime", types.Float(), help="customizable warming time"),
                                        keys.Key("halogen", types.Int(), help="requested quartz halogen lamp time"),
                                        keys.Key("argon", types.Int(), help="requested Ar lamp time"),
                                        keys.Key("neon", types.Int(), help="requested Ne lamp time"),
                                        keys.Key("krypton", types.Int(), help="requested Kr lamp time"),
                                        keys.Key("xenon", types.Int(), help="requested Xenon lamp time"),
                                        keys.Key("hgar", types.Int(), help="requested HgAr lamp time"),
                                        keys.Key("hgcd", types.Int(), help="requested HgCd lamp time"),
                                        keys.Key("delay", types.Float(), help="delay before turning lamps on"),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError(f'{self.name} controller is not connected.')

    @property
    def lampNames(self):
        return self.controller.lampNames

    @property
    def config(self):
        return self.controller.config

    @property
    def lampString(self):
        """Describe the lamps configured by the .prepare() command."""
        ll = [] if self.config is None else ["%s=%0.1f" % (ln, lv) for ln, lv in self.config.items()]
        return ','.join(ll)

    @threaded
    def status(self, cmd):
        """Report state, mode, status."""
        self.controller.generate(cmd)

    @singleShot
    def warmup(self, cmd):
        """Switch on light lamps and warm it up if requested, FSM protect from go command."""
        cmdKeys = cmd.cmd.keywords

        lampsOn = cmdKeys['on'].values if 'on' in cmdKeys else []
        warmingTime = cmdKeys['warmingTime'].values[0] if 'warmingTime' in cmdKeys else None
        warmingTime = 0 if 'force' in cmdKeys else warmingTime

        for name in lampsOn:
            if name not in self.lampNames:
                raise ValueError(f'{name} : unknown lamp')

        self.controller.substates.warming(cmd, lamps=lampsOn, warmingTime=warmingTime)
        self.controller.generate(cmd)

    @blocking
    def switchOff(self, cmd):
        """Switch off light lamps."""
        cmdKeys = cmd.cmd.keywords
        lampsOff = cmdKeys['off'].values

        for name in lampsOff:
            if name not in self.lampNames:
                raise ValueError(f'{name} : unknown lamp')

        self.controller.switchOff(cmd, lampsOff)
        self.controller.generate(cmd)

    @blocking
    def prepare(self, cmd):
        """Configure a future illumination sequence."""
        cmdKeys = cmd.cmd.keywords

        if self.config is not None:
            cmd.warn('text="active lamp configuration being overwritten (%s)"' % self.lampString)

        self.config.clear()

        # fetching all possible lamps configuration
        toPrepare = [lamp for lamp in lampState.allLamps if lamp in cmdKeys]

        for lamp in toPrepare:
            if lamp not in self.lampNames:
                raise ValueError(f'unknown lamp {lamp}, lampNames={",".join(self.lampNames)}')

            self.config[lamp] = int(cmdKeys[lamp].values[0])

        self.controller.prepare(cmd)

        cmd.finish('text="will turn on: %s"' % self.lampString)

    @blocking
    def _go(self, cmd, delay):
        """Run the preconfigured illumination sequence.

        Note
        ----
        Currently don't clear the predefined sequence.
        """
        lamps = tuple(self.config.keys())

        if delay > 0:
            cmd.debug(f'text="will turn on {lamps} in {delay}s seconds"')
            time.sleep(delay)

        if not self.controller.abortWarmup:
            self.controller.substates.triggering(cmd)

        self.controller.generate(cmd)

    def go(self, cmd):
        """ Start go command, return until completion. """
        cmdKeys = cmd.cmd.keywords
        delay = cmdKeys['delay'].values[0] if 'delay' in cmdKeys else 0.0
        noWait = 'noWait' in cmdKeys

        if self.controller.isLocked:
            raise RuntimeWarning(f'{self.controller.name} thread is locked')

        if self.config is None or len(self.config) == 0:
            cmd.fail('text="no lamps are configured to turn on now"')
            self.config.clear()
            return

        # just finish command now and proceed.
        if noWait:
            cmd.finish('text="return immediately"')
            cmd = self.actor.bcast

        self._go(cmd, delay=delay)

    def abort(self, cmd):
        """Abort iis warmup."""
        self.controller.doAbort()
        cmd.finish('text="warmup aborted"')

    def waitForReadySignal(self, cmd):
        """to be consistent with pfilamps."""
        if self.config is None or len(self.config) == 0:
            cmd.fail('text="no lamps are configured to turn on now"')
            self.config.clear()
            return

        now = pfsTime.timestamp()
        needWarmup = dict([(lamp, self.controller.lampStates[lamp].needWarmup(now)) for lamp in self.config.keys()])
        lamps = [lamp for lamp, warmingTime in needWarmup.items() if warmingTime]

        if lamps:
            self.controller.substates.warming(cmd, lamps=lamps, warmingTime=max(needWarmup.values()))
            self.controller.switchOff(cmd, lamps)

        cmd.finish('text="lamps are ready"')

    @singleShot
    def stop(self, cmd):
        """Abort iis warmup, turn iis lamp off and disconnect."""
        self.actor.disconnect(self.name, cmd=cmd)
        cmd.finish()

    @singleShot
    def start(self, cmd):
        """Wait for pdu host, connect iis controller."""
        cmdKeys = cmd.cmd.keywords
        mode = self.actor.actorConfig[self.name]['mode']
        host = self.actor.actorConfig[self.name]['host']
        port = self.actor.actorConfig[self.name]['port']
        mode = 'operation' if 'operation' in cmdKeys else mode
        mode = 'simulation' if 'simulation' in cmdKeys else mode

        if mode == 'operation':
            # no need to do that in simulation.
            tcpUtils.waitForTcpServer(host=host, port=port, cmd=cmd)

        cmd.inform('text="connecting lamps..."')
        self.actor.connect(self.name, cmd=cmd, mode=mode)
        cmd.finish()
