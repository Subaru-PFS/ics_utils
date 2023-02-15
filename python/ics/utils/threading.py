import time
from functools import partial

import actorcore.Actor as coreActor
from actorcore.QThread import QThread
from ics.utils.fsm.fsmThread import LockedThread


def getThread(instance, threadClass=QThread):
    """ Be robust about being called from QThread itself or the ActorCmd.py"""
    if isinstance(instance, threadClass):
        return instance
    elif hasattr(instance, 'controller') and isinstance(instance.controller, threadClass):
        return instance.controller
    else:
        raise RuntimeError('havent found any available thread to put func on')


def putMsg(func):
    def wrapper(self, cmd, *args, **kwargs):
        thread = getThread(self)
        thread.putMsg(partial(func, self, cmd, *args, **kwargs))

    return wrapper


def putAndExit(func):
    def wrapper(self, cmd, *args, **kwargs):
        if isinstance(self, coreActor.Actor):
            actor = self
        elif hasattr(self, 'actor'):
            actor = self.actor
        else:
            raise RuntimeError('this must run within an actor.')

        thr = QThread(actor, str(time.time()))
        thr.start()
        thr.putMsg(partial(func, self, cmd, *args, **kwargs))
        thr.exitASAP = True

    return wrapper


def mhsFunc(func):
    def wrapper(self, cmd, *args, **kwargs):
        try:
            return func(self, cmd, *args, **kwargs)
        except Exception as e:
            cmd.fail('text=%s' % self.actor.strTraceback(e))

    return wrapper


def threaded(func):
    @putMsg
    @mhsFunc
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def singleShot(func):
    @putAndExit
    @mhsFunc
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def checkAndPut(func):
    def wrapper(self, cmd, *args, **kwargs):
        thread = getThread(self, threadClass=LockedThread)
        if thread.isLocked:
            raise RuntimeWarning(f'{thread.name} is busy')

        thread.putMsg(partial(func, self, cmd, *args, **kwargs))

    return wrapper


def blocking(func):
    # Note that To be used with FsmThread or at least LockThread or it will blow off.
    @checkAndPut
    @mhsFunc
    def wrapper(self, cmd, *args, **kwargs):
        thread = getThread(self, threadClass=LockedThread)
        thread.lock(cmd)
        try:
            return func(self, cmd, *args, **kwargs)
        finally:
            thread.unlock()

    return wrapper
