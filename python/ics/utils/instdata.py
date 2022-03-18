import pfs.instdata.io as fileIO
from ics.utils.actors import findProductAndInstance


class InstConfig(dict):
    """Inst Config class to handle per-actor config yaml file."""

    def __init__(self, actorName):
        super().__init__()
        productName, instanceName = findProductAndInstance(actorName)
        self.idDict = dict(actorName=actorName, productName=productName, instanceName=instanceName)
        self.reload()

    @property
    def productName(self):
        return self.idDict['productName']

    @property
    def instanceName(self):
        return self.idDict['instanceName']

    @property
    def filepath(self):
        # retrieve config filepath, can be useful for book-keeping.
        return fileIO.absFilepath('config', 'actors', self.productName)

    def reload(self):
        """Reload YAML configuration file and update dictionary."""

        try:
            config = fileIO.loadConfig(self.productName, subDirectory='actors')
            # load per instance config if that make sense.
            config = config[self.instanceName] if self.instanceName is not None else config

        except (FileNotFoundError, KeyError):
            config = dict()

        self.update(config)
        # if string interpolation is enabled
        self.interpolate()

    def enableStringInterpolation(self, idDict):
        """Enable string interpolation for config file

        Parameters
        ----------
        idDict : `dict`
           identifier dictionary.
        """

        self.idDict.update(idDict)
        self.reload()

    def interpolate(self):
        """ interpolate configuration file with identificator dict.
        Note that the interpolation is done in place."""

        for __, field in self.items():
            interpolated = dict()

            for key, val in field.items():
                if isinstance(val, str):
                    interpolated[key] = val.format(**self.idDict)

            field.update(interpolated)


class InstData(object):

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

    @staticmethod
    def loadActorData(actorName):
        """ Load persisted actor keyword from outside mhs world. """
        return fileIO.loadData(actorName, subDirectory='actors')

    @staticmethod
    def loadPersisted(actorName, keyName):
        """ Load persisted actor keyword from outside mhs world. """
        return InstData.loadActorData(actorName)[keyName]

    def loadKey(self, keyName, actorName=None, cmd=None):
        """ Load mhs keyword values from disk.

        Args
        ----
        keyName : `str`
            Keyword name.
        """
        cmd = self.actor.bcast if cmd is None else cmd
        actorName = self.actorName if actorName is None else actorName
        cmd.inform(f'text="loading {keyName} from instdata"')

        return InstData.loadPersisted(actorName, keyName)

    def loadKeys(self, actorName=None, cmd=None):
        """ Load all keys values from disk. """

        cmd = self.actor.bcast if cmd is None else cmd
        actorName = self.actorName if actorName is None else actorName
        cmd.inform(f'text="loading keys from instdata"')

        return InstData.loadActorData(actorName)

    def persistKey(self, keyName, *values, cmd=None):
        """ Save single mhs keyword values to disk.

        Args
        ----
        keyName : `str`
            Keyword name.
        """
        cmd = self.actor.bcast if cmd is None else cmd
        data = dict([(keyName, values)])

        self._persist(data)
        cmd.inform(f'text="dumped {keyName} to instdata"')

    def persistKeys(self, keys, cmd=None):
        """ Save mhs keyword dictionary to disk.

        Args
        ----
        keys : `dict`
            Keyword dictionary.
        """
        cmd = self.actor.bcast if cmd is None else cmd

        self._persist(keys)
        cmd.inform(f'text="dumped keys to instdata"')

    def _persist(self, keys, cmd=None):
        """ Load and update persisted data.
        Create a new file if it does not exist yet.

        Args
        ----
        keys : `dict`
            Keyword dictionary.
        """
        cmd = self.actor.bcast if cmd is None else cmd

        try:
            data = self.loadKeys(self.actorName)
        except FileNotFoundError:
            cmd.warn(f'text="instdata : {self.actorName} file does not exist, creating empty file"')
            data = dict()

        data.update(keys)
        fileIO.dumpData(self.actorName, data, subDirectory='actors')
