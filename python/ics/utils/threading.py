import time
from functools import partial

from actorcore.QThread import QThread


def getThread(instance):
    """ Be robust about being called from QThread itself or the ActorCmd.py"""
    if isinstance(instance, QThread):
        return instance
    elif hasattr(instance, 'controller') and isinstance(instance.controller, QThread):
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
        thr = QThread(self.actor, str(time.time()))
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


class locking:
    """ There is no safe way to know whether a QThread is actually doing something.
        Its either waiting from queue.get(timeout=..) or calling a function. this class is basically a workaround. """

    @staticmethod
    def isLocked(thread):
        try:
            return thread.onGoingCmd
        except AttributeError:
            return False

    @staticmethod
    def lock(thread, cmd):
        thread.onGoingCmd = cmd

    @staticmethod
    def unlock(thread):
        thread.onGoingCmd = False


def checkAndPut(func):
    def wrapper(self, cmd, *args, **kwargs):
        thread = getThread(self)
        if locking.isLocked(thread):
            raise RuntimeWarning(f'{thread.name} is busy')

        thread.putMsg(partial(func, self, cmd, *args, **kwargs))

    return wrapper


def blocking(func):
    @checkAndPut
    @mhsFunc
    def wrapper(self, cmd, *args, **kwargs):
        thread = getThread(self)
        locking.lock(thread, cmd)
        try:
            return func(self, cmd, *args, **kwargs)
        finally:
            locking.unlock(thread)

    return wrapper
