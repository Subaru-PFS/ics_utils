from ics.utils.sps.parts import VisCam, NirCam, Shutter, Rda, Fca, Bia, Iis
from ics.utils.sps.spectroIds import SpectroIds


class LightSource(str):
    validNames = ['dcb', 'dcb2', 'sunss', 'afl9mtp', 'afl12mtp', 'pfi', 'none']
    """Class to describe lightSource, fairly minimal for now."""

    def __new__(cls, name):
        return str.__new__(cls, str(name).lower())

    @property
    def lampsActor(self):
        if self in ['dcb', 'dcb2']:
            return self
        elif self in ['afl9mtp', 'afl12mtp']:
            return 'dcb'  # allFiberLamp is connected to dcb pdu.
        elif self == 'pfi':
            return 'pfilamps'
        elif self in ['sunss', 'none']:
            return None
        else:
            raise ValueError(f'unknown lampsActor for {self}')

    @property
    def useDcbActor(self):
        return self in ['dcb', 'dcb2'] + ['afl9mtp', 'afl12mtp']


class NoShutterException(Exception):
    """Exception raised when an exposure is required without any working shutter to ensure exposure time.

    Attributes
    ----------
    text : `str`
       Exception text.
    """

    def __init__(self, text):
        Exception.__init__(self, text)


class SpecModule(SpectroIds):
    """Placeholder to handle a single spectrograph module configuration, lightSource, parts...
    It also describe if this module is part of the spectrograph system or standalone module.

    Attributes
    ----------
    specName : `str`
        Spectrograph module identifier (sm1, sm2, ...).
    spsModule : `bool`
        Is this module actually part of the spectrograph system (sps), or standalone.
    lightSource : `str`
        The light source which is feeding the spectrograph module.
    """
    knownParts = ['bcu', 'rcu', 'ncu', 'bsh', 'rsh', 'fca', 'rda', 'bia', 'iis']
    armToFpa = dict(b='b', m='r', r='r', n='n')
    validNames = [f'sm{specNum}' for specNum in SpectroIds.validModules]

    def __init__(self, specName, spsModule=True, lightSource=None):
        SpectroIds.__init__(self, specName)
        self.spsModule = spsModule
        self.lightSource = LightSource(lightSource)

    @property
    def cams(self):
        """Camera dictionary for a given spectrograph module."""
        return dict([(cam.arm, cam) for cam in [self.bcu, self.rcu, self.ncu]])

    @property
    def parts(self):
        """All existing spectrograph module parts, basically camera + entrance unit parts."""
        return list(self.cams.values()) + [self.bsh, self.rsh, self.fca, self.rda, self.bia, self.iis]

    @property
    def opeSubSys(self):
        return [part for part in self.parts if part.operational]

    @property
    def genSpecParts(self):
        """Generate string that describe the spectrograph module parts."""
        return f'{self.specName}Parts={",".join([part.state for part in self.parts])}'

    @property
    def genLightSource(self):
        """Generate string that describe the spectrograph light source."""
        return f'{self.specName}LightSource={self.lightSource}'

    @property
    def enuName(self):
        return f'enu_{self.specName}'

    @classmethod
    def fromConfig(cls, specName, config, spsData):
        """Instantiate SpecModule class from spsActor.configParser.

        Parameters
        ----------
        specName : `str`
            Spectrograph module identifier (sm1, sm2, ...).
        config : `spsActor.configParser`
            ConfigParser object from spsActor.
        spsData : `ics.utils.instdata.InstData`
            Sps instrument data object.

        Returns
        -------
        specModule : `SpecModule`
            SpecModule object.
        """
        try:
            lightSource, = spsData.loadKey(f'{specName}LightSource')
        except:
            lightSource = None

        try:
            spsModules = config['spsModules']
        except:
            spsModules = [specName for specName in SpecModule.validNames if specName in config.keys()]

        spsModule = specName in spsModules
        specConfig = config[specName]
        specModule = cls(specName, spsModule=spsModule, lightSource=lightSource)

        parts = dict()
        for partName in SpecModule.knownParts:
            state = specConfig.get(partName, 'none')
            parts[partName] = state

        specModule.assign(**parts)
        return specModule

    @classmethod
    def fromModel(cls, specName, spsModel):
        """Instantiate SpecModule class from spsActor model.

        Parameters
        ----------
        specName : `str`
            Spectrograph module identifier (sm1, sm2, ...).
        spsModel : `opscore.actor.model.Model`
            SpsActor model.

        Returns
        -------
        specModule : `SpecModule`
            SpecModule object.
        """
        spsModule = specName in spsModel.keyVarDict['spsModules'].getValue()
        specParts = spsModel.keyVarDict[f'{specName}Parts'].getValue()
        lightSource = spsModel.keyVarDict[f'{specName}LightSource'].getValue()

        specModule = cls(specName, spsModule=spsModule, lightSource=lightSource)
        specModule.assign(*specParts)

        return specModule

    def getCams(self, filter='default'):
        """Return all camera objects given a filter.

        Parameters
        ----------
        filter : `str`
            how to filter, if default return all camera described as sci.

        Returns
        -------
        cam : `ics.utils.sps.part.Cam`
            Cam object.
        """
        if filter == 'default':
            cams = [cam for cam in self.cams.values() if cam.default]
        elif filter == 'operational':
            cams = [cam for cam in self.cams.values() if cam.operational]
        else:
            raise ValueError(f'unknown filter:{filter}')

        return cams

    def assign(self, bcu='none', rcu='none', ncu='none', bsh='none', rsh='none', fca='none', rda='none', bia='none',
               iis='none'):
        """Instantiate and assign each part from the provided operating state.

        Parameters
        ----------
        bcu : `str`
            Blue camera operating state.
        rcu : `str`
            Red camera operating state.
        ncu : `str`
            Nir camera operating state.
        bsh : `str`
            Blue shutter operating state.
        rsh : `str`
            Red Shutter operating state.
        fca : `str`
            Fiber Cable A (hexapod) operating state.
        rda : `str`
            Red exchange mechanism operating state.
        bia : `str`
            Back Illumination Assembly operating state
        iis : `str`
            Internal Illumination Sources operating state.
        """
        self.bcu = VisCam(self, 'b', bcu)
        self.rcu = VisCam(self, 'r', rcu)
        self.ncu = NirCam(self, ncu)
        self.bsh = Shutter(self, 'b', bsh)
        self.rsh = Shutter(self, 'r', rsh)
        self.fca = Fca(self, fca)
        self.rda = Rda(self, rda)
        self.bia = Bia(self, bia)
        self.iis = Iis(self, iis)

    def camera(self, arm):
        """Return Cam object from arm.

        Parameters
        ----------
        arm : `str`
            spectrograph module arm(b,r,n,m)

        Returns
        -------
        cam : `ics.utils.sps.part.Cam`
            Cam object.
        """
        if arm not in SpecModule.validArms:
            raise RuntimeError(f'arm {arm} must be one of: {list(SpecModule.validArms.keys())}')

        fpa = SpecModule.armToFpa[arm]
        cam = self.cams[fpa]

        if not cam.operational:
            raise RuntimeError(f'{str(cam)} cam state: {cam.state}, not operational ...')

        return cam

    def lightSolver(self, arm, openShutter=True):
        """ In the spectrograph, for blue arm light goes through two shutters but only one for the other arms.
        This function simulate what's the output light for a given set of shutters and a continuous input light.

        Parameters
        ----------
        arm : `str`
            Spectrograph arm.
        openShutter : `bool`
            Shutter is required to open.

        Returns
        -------
        outputLight : `str`
             Output light beam(continuous, timed, none, unknown).
        shutterSet : list of `ics.utils.sps.part.Shutter`
            List of matching shutters.

        """
        inputLight = 'continuous'
        shutterSet = [self.rsh, self.bsh] if arm == 'b' else [self.rsh]

        for shutter in shutterSet:
            outputLight = shutter.lightPath(inputLight, openShutter=openShutter)
            inputLight = outputLight

        return outputLight, shutterSet

    def shutterSet(self, arm, lightBeam):
        """Return the required shutter set for a given arm and lightBeam.
        Check that what you want to measure is actually what you get.

        Parameters
        ----------
        arm : `str`
            Spectrograph arm.
        lightBeam : `bool`
            Are you measuring photons ?

        Returns
        -------
        requiredShutters : list of `ics.utils.sps.part.Shutter`
            List of required shutters.
        """
        outputLight, shutterSet = self.lightSolver(arm, openShutter=lightBeam)

        try:
            if outputLight == 'continuous':
                raise NoShutterException(f"cannot control exposure on {arm} arm...")

            # never been used so far and might be in the end overkill...

            # elif outputLight == 'none':
            #     if lightBeam:
            #         raise RuntimeError(f'light cant reach {arm} arm...')
            # elif outputLight == 'unknown':
            #     raise RuntimeError(f'cannot guaranty anything on {arm} arm')

        except NoShutterException:
            # Just assume people knows what they are doing, if there are no shutters return nothing.
            return []

        opeShutters = [shutter for shutter in shutterSet if shutter.operational]
        requiredShutters = opeShutters if lightBeam else opeShutters[-1:]

        # not a fan of this implementation since it leaves state behind, but I think that's safe.
        for shutter in requiredShutters:
            shutter.setLightBeam(lightBeam)

        return requiredShutters

    def dependencies(self, arm, seqObj):
        """Retrieve the spectrograph dependencies given the arm and the data acquisition sequence type.
        Only implemented shutter dependencies for know.

        Parameters
        ----------
        arm : `str`
            Spectrograph arm.
        seqObj : `iicActor.sps.sequence.Sequence`
           Sequence instance.

        Returns
        -------
        names : `list` of `Part`
            List of required parts.
        """

        def specDeps(arm, lightBeam):
            """Return lamps/fca/bia/rda dependencies for a given arm and lightBeam."""
            # start with the camera itself.
            deps = [self.cams[arm]]
            # lock spectrograph subsystems, note that shutters are dealt separately.
            if lightBeam:
                deps += [self.fca, self.bia]
                deps += [self.rda] if arm in ['r', 'm'] else []

                # adding lampActor, will be None for SuNSS.
                deps += [self.lightSource.lampsActor] if self.lightSource.lampsActor else []

            return deps

        # deps = spectroDeps + shutters
        deps = specDeps(arm, seqObj.lightBeam)
        deps.extend(self.shutterSet(arm, seqObj.lightBeam))

        return deps

    def askAnEngineer(self, seqObj):
        """If NoShutterException is raised, check for special cases, timed dcb exposure is one of them.

        Parameters
        ----------
        seqObj : `iicActor.sps.sequence.Sequence`
           Sequence instance.

        Returns
        -------
        names : `list` of `Part`
            List of required parts.
        """
        if 'dcb' in self.lightSource:
            if seqObj.lightBeam:
                if not seqObj.shutterRequired:
                    return []
            else:
                return [self.lightSource]

        elif 'sunss' in self.lightSource:
            return []

        raise


class SpsConfig(dict):
    """Placeholder spectrograph system configuration in mhs world.

    Attributes
    ----------
    specModules : list of `SpecModule`
        List of described and instanciated spectrograph module.
    """
    validCams = [f'{arm}{specNum}' for arm in SpectroIds.validArms.keys() for specNum in SpectroIds.validModules]

    def __init__(self, specModules):
        super().__init__()
        for specModule in specModules:
            self[specModule.specName] = specModule

    @property
    def spsModules(self):
        """Spectrograph modules labelled as part of the spectrograph system(sps)"""
        return dict([(name, module) for name, module in self.items() if module.spsModule])

    @classmethod
    def fromConfig(cls, spsActor):
        """Instantiate SpsConfig class from spsActor.configParser.
        Instantiate only SpecModule which are described in the configuration file.

        Parameters
        ----------
        spsActor : `actorcore.ICC.ICC`
            spsActor.

        Returns
        -------
        spsConfig : `SpsConfig`
            SpsConfig object.
        """
        localConfig = spsActor.actorConfig[spsActor.site]
        specNames = [specName for specName in SpecModule.validNames if specName in localConfig.keys()]
        specModules = [SpecModule.fromConfig(specName, localConfig, spsActor.actorData) for specName in specNames]

        return cls([specModule for specModule in specModules])

    @classmethod
    def fromModel(cls, spsModel):
        """Instantiate SpsConfig class from spsActor model.
        Instantiate only SpecModule which are in specModules.

        Parameters
        ----------
        spsModel : `opscore.actor.model.Model`
            SpsActor model.

        Returns
        -------
        spsConfig : `SpsConfig`
            SpsConfig object.
        """
        specNames = spsModel.keyVarDict['specModules'].getValue()
        return cls([SpecModule.fromModel(specName, spsModel) for specName in specNames])

    def identify(self, cams=None, arms=None, specNums=None, filter='default'):
        """Identify which camera(s) to expose from outer product(specNums*arm) or cams.
        If no specNums is provided then we're assuming modules labelled as sps.
        If no arm is provided then we're assuming all arms.

        Parameters:
        specNums : list of `int`
            List of required spectrograph module number (1,2,...).
        arms : list of `str`
            List of required arm (b,r,n,m)
        cams : list of `str`
            List of camera names.
        filter : str, optional
            Filter to select cameras.
            Default is 'default', which selects all cameras defined as science in the config file.
            If set to 'operational', all cameras defined as science or engineering are selected.

        Returns:
        cams : `list` of `Cam`
            List of Cam object.
        """
        if cams is None:
            specModules = self.selectModules(specNums)
            cams = self.selectArms(specModules, arms, filter=filter)
        else:
            cams = [self.selectCam(camName) for camName in cams]

        return cams

    def selectModules(self, specNums=None):
        """Select spectrograph modules for a given list of spectrograph number.

        Parameters:
        specNums : list of `int`
            List of required spectrograph module number (1,2,..).

        Returns:
        specModules : `list` of `SpecModule`
            List of SpecModule object.
        """
        specModules = []

        if specNums is None:
            return list(self.spsModules.values())

        for specNum in specNums:
            try:
                specModules.append(self[f'sm{specNum}'])
            except KeyError:
                raise RuntimeError(f'sm{specNum} is not wired in, specModules={",".join(self.keys())}')

        return specModules

    def selectArms(self, specModules, arms=None, filter='default'):
        """Return the outer product between provided specModules and arms.
        If no arms is provided, then assuming all arms.

        Parameters:
        specModules : list of `specModules`
            List of required spectrograph module.
        arms : list of `str`, optional
            List of required arm (b,r,n,m). If not provided, all arms are assumed.
        filter : str, optional
            Filter to select cameras.
            Default is 'default', which selects all cameras defined as science in the config file.
            If set to 'operational', all cameras defined as science or engineering are selected.

        Returns:
        cams : `list` of `Cam`
            List of Cam object.
        """
        arms = SpectroIds.validArms if arms is None else arms
        # retrieve all cams first.
        cams = sum([specModule.getCams(filter=filter) for specModule in specModules], [])
        # filter by arm.
        cams = [cam for cam in cams if cam.arm in arms]

        return cams

    def selectCam(self, camName):
        """Retrieve Cam object from camera name.

        Parameters:
        camName : `str`
            Camera name.

        Returns:
        cam : `ics.utils.sps.part.Cam`
            Cam object.
        """
        if camName not in SpsConfig.validCams:
            raise ValueError(f'{camName} is not a valid cam')

        arm, specNum = camName[0], int(camName[1])
        [cam] = self.identify(specNums=[specNum], arms=[arm], filter='operational')

        return cam

    def declareLightSource(self, lightSource, specNum=None, spsData=None):
        """Declare light source for a given spectrograph number.
        if no spectrograph number is provided, then we're assuming that light is declared for sps.
        The only light source which can feed multiple spectrograph modules is pfi.
        The other sources are unassigned before getting reassigned to another spectrograph module.

        Parameters
        ----------
        lightSource : `str`
        The light source which is feeding the spectrograph module.
        specNum : `int`
            Spectrograph module number (1,2,..).
        spsData : `ics.utils.instdata.InstData`
            Sps instrument data object.
        """
        if lightSource not in LightSource.validNames:
            raise RuntimeError(f'lightSource: {lightSource} must be one of: {",".join(LightSource.validNames)}')

        specModules = self.selectModules([specNum]) if specNum is not None else self.spsModules.values()

        if lightSource != 'pfi':
            # for now, I declare that to be true, might change in the future.
            if len(specModules) > 1:
                raise RuntimeError(f'{lightSource} can only be plugged to a single SM')

            # other light source can only plug into one sm, so you need to undeclare it first.
            toUndeclare = [module for module in self.values() if module.lightSource == lightSource]
            for specModule in toUndeclare:
                spsData.persistKey(f'{specModule.specName}LightSource', None)

        for specModule in specModules:
            spsData.persistKey(f'{specModule.specName}LightSource', lightSource)

    def keysToCam(self, cmdKeys):
        """
        Identify the cameras based on the provided command keywords.

        Parameters:
        cmdKeys (opscore.protocols.messages.Keywords): Command keywords.

        Returns:
        list: A list of identified cameras.
        """
        # identify cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else None
        cams = cmdKeys['cam'].values if 'cam' in cmdKeys else cams
        # identify specNums
        specNums = cmdKeys['specNums'].values if 'specNums' in cmdKeys else None
        specNums = cmdKeys['specNum'].values if 'specNum' in cmdKeys else specNums
        # identify arms
        arms = cmdKeys['arms'].values if 'arms' in cmdKeys else None
        arms = cmdKeys['arm'].values if 'arm' in cmdKeys else arms

        if cams and (specNums or arms):
            raise ValueError('you cannot provide both cam and (specNum or arm)')

        return self.identify(cams=cams, specNums=specNums, arms=arms)

    def keysToSpecNum(self, cmdKeys):
        """
        Get the specNum from command keywords if specified, or get values from spsConfig otherwise.

        Parameters:
        cmdKeys (opscore.protocols.messages.Keywords): Command keywords.

        Returns:
        list: A list of unique specNums.
        """
        # identify cams
        cams = self.keysToCam(cmdKeys)
        # get unique specNums
        return list(set([cam.specNum for cam in cams]))
