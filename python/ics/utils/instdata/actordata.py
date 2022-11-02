import ics.utils.instdata.io as instdataIO


class ActorData(object):

    def __init__(self, actor):
        """ Load /save mhs keywords values from/to disk.

        Args
        ----
        actor : actorcore.Actor object
            a running actor instance.
        """
        self.actor = actor

    @property
    def actorName(self):
        return self.actor.name

    def getCmd(self, cmd=None):
        """Return cmd object."""
        return self.actor.bcast if cmd is None else cmd

    def loadKeys(self, actorName=None, cmd=None):
        """ Load all keys values from disk. """
        actorName = self.actorName if actorName is None else actorName
        return instdataIO.loadYaml('/data', actorName, subDirectory='actors', isRelative=False)

    def loadKey(self, keyName, actorName=None, cmd=None):
        """ Load mhs keyword values from disk.

        Args
        ----
        keyName : `str`
            Keyword name.
        """
        self.getCmd(cmd).inform(f'text="loading {keyName} from {self.actorName} repo"')
        return self.loadKeys(actorName)[keyName]

    def persistKeys(self, keys, cmd=None):
        """ Save mhs keyword dictionary to disk.

        Args
        ----
        keys : `dict`
            Keyword dictionary.
        """
        self._persist(keys)
        self.getCmd(cmd).inform(f'text="dumped {",".join(keys.keys())} to {self.actorName} repo"')

    def persistKey(self, keyName, *values, cmd=None):
        """ Save single mhs keyword values to disk.

        Args
        ----
        keyName : `str`
            Keyword name.
        """
        data = dict([(keyName, values)])
        self.persistKeys(data, cmd=cmd)

    def _persist(self, keys, cmd=None):
        """ Load and update persisted data.
        Create a new file if it does not exist yet.

        Args
        ----
        keys : `dict`
            Keyword dictionary.
        """
        try:
            data = self.loadKeys(self.actorName)
        except FileNotFoundError:
            self.getCmd(cmd).warn(f'text="/data/actors/{self.actorName}.yaml does not exist, creating empty file"')
            data = dict()

        data.update(keys)
        instdataIO.dumpYaml('/data', self.actorName, data, subDirectory='actors', isRelative=False)
