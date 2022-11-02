import logging

import ics.utils.instdata.io as instdataIO
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
        return instdataIO.toAbsFilepath('config', 'actors', self.productName)

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
