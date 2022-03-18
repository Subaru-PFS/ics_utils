from ics.utils import time as pfsTime

def isoTs(t=None):
    """Filthy function to get a formatted time string. """
    if t is None:
        ts = pfsTime.Time.now()
    else:
        ts = pfsTime.Time.fromtimestamp(t)

    return ts.isoformat()

class NullCmd:
    """Dummy Command for when we are not using actorcore code."""
    def print(self, level, s):
        print(f'{isoTs()} {level} {s}')

    def diag(self, s):
        self.print('d', s)
    def inform(self, s):
        self.print('i', s)
    def warn(self, s):
        self.print('w', s)
    def finish(self, s=""):
        self.print(':', s)
    def fail(self, s=""):
        self.print('f', s)
