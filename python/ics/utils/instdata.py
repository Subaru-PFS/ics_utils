import logging

import pfs.instdata.io as instdataIO
from ics.utils.actors import findProductAndInstance


class InstConfig(dict):
    """Inst Config class to handle per-actor config yaml file."""

    def __init__(self, actorName, idDict=None, logLevel=20):
        super().__init__()
        self.logger = logging.getLogger('config')
        self.logger.setLevel(logLevel)

        self.idDict = self.fetchIds(actorName, idDict=idDict)
        self.load()

    @property
    def productName(self):
        return self.idDict['productName']

    @property
    def instanceName(self):
        return self.idDict['instanceName']

    @property
    def filepath(self):
        # retrieve config filepath, can be useful for book-keeping.
        return instdataIO.absFilepath('config', 'actors', self.productName)

    def fetchIds(self, actorName, idDict=None):
        """fetch ids from product and optional id dictionary."""
        idDict = dict() if idDict is None else idDict
        productName, instanceName = findProductAndInstance(actorName)
        idDict.update(actorName=actorName, productName=productName, instanceName=instanceName)
        return idDict

    def loadConfig(self):
        """Load per-actor yaml file."""
        try:
            config = instdataIO.loadConfig(self.productName, subDirectory='actors')
            # load per instance config, sometimes instanceName==productName, but there is always an instance.
            config = config[self.instanceName]

        except (FileNotFoundError, KeyError):
            config = dict()

        return config

    def load(self):
        """load YAML configuration file and update dictionary."""

        instConfig = self.loadConfig()
        actorsConfig = instdataIO.loadConfig('actors', subDirectory='actors')

        # try to find similar section in our instConfig file.
        for section, cfgDict in actorsConfig.items():
            try:
                specified = instConfig[section]
            except KeyError:
                specified = dict()
            # update config dictionary with overloaded values.
            cfgDict.update(specified)
            instConfig[section] = cfgDict

        self.update(instConfig)
        # if string interpolation is enabled
        self.interpolate()

    def reload(self):
        """Reload configuration dynamically."""
        self.clear()
        self.load()

    def interpolate(self, idDict=None):
        """ interpolate configuration file with identificator dict.
        Note that the interpolation is done in place.
        
        Parameters
        ----------
        idDict : `dict`
           optional identifier dictionary.
        """
        # if provided update current identifier dictionary.
        if isinstance(idDict, dict):
            self.idDict.update(idDict)

        def recursiveInterp(d):
            """Do a recursive interpolation to deal with nested dictionaries."""
            for key, val in d.items():
                if isinstance(val, dict):
                    recursiveInterp(val)
                elif isinstance(val, str):
                    try:
                        d[key] = val.format(**self.idDict)
                    except KeyError:
                        self.logger.warning(f'could not interpolate {val} from {self.idDict}')

        # just call it on itself.
        recursiveInterp(self)


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
        return instdataIO.loadData(actorName, subDirectory='actors')

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
        instdataIO.dumpData(self.actorName, data, subDirectory='actors')
