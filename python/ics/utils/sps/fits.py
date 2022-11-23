""" SPS-specific FITS routines. """

from ics.utils.fits import wcs
from ics.utils.fits import timecards
from ics.utils.fits import mhs as fitsUtils

# These should come from some proper data product, but I would be
# *very* surprised if the values matter much. They certainly do
# not matter for PFS itself.
#
# The table values were gathered from ETC data and some grating design docs.
#
armSpecs = dict(b=dict(wavemin=380.0,
                       wavemax=650.0,
                       wavemid=519.0,
                       fringe=711.0),
                r=dict(wavemin=630.0,
                       wavemax=970.0,
                       wavemid=806.0,
                       fringe=557.0),
                m=dict(wavemin=710.0,
                       wavemax=885.0,
                       wavemid=1007.0,
                       fringe=1007.0),
                n=dict(wavemin=940.0,
                       wavemax=1260.0,
                       wavemid=1107.0,
                       fringe=1007.0))

def getSpsSpectroCards(arm):
    """Return the Subaru-specific spectroscopy cards.

    See INSTRM-1022 for gory discussion. We might need to add other
    cards.

    Args
    ----
    arm : `str`
      the letter for the arm we are interested in. 'brnm'

    Returns
    -------
    cards : list of fitsio-compliant card dicts.

    """

    cards = []
    try:
        specs = armSpecs[arm]
    except KeyError:
        raise ValueError(f'arm must be one of "brnm", not {arm}')

    disperserName = f'VPH_{arm}_{int(specs["fringe"])}_{int(specs["wavemid"])}nm'
    cards.append(dict(name='DISPAXIS', value=(1 if arm == 'n' else 2),
                      comment='Dispersion axis (along %s)' % ('rows' if arm == 'n' else 'columns')))
    cards.append(dict(name='DISPERSR', value=disperserName, comment='Disperser name (arm_fringe/mm_centralNm)'))
    cards.append(dict(name='WAV-MIN', value=specs['wavemin'], comment='[nm] Blue edge of the bandpass'))
    cards.append(dict(name='WAV-MAX', value=specs['wavemax'], comment='[nm] Red edge of the bandpass'))
    cards.append(dict(name='WAVELEN', value=specs['wavemid'], comment='[nm] Middle of the bandpass'))

    cards.append(dict(name='SLIT', value='PFS', comment='Identifier of entrance slit'))
    cards.append(dict(name='SLT-LEN', value=1.05, comment='[arcsec] Fiber diameter'))
    cards.append(dict(name='SLT-WID', value=1.05, comment='[arcsec] Fiber diameter'))

    return cards

def getSpsWcs(arm):
    """Return a Subaru-compliant WCS solution."""

    raise NotImplementedError("Sorry, no SPS WCS yet!")

class SpsFits:
    def __init__(self, actor, cmd, exptype):
        self.actor = actor
        self.cmd = cmd
        self.exptype = exptype

    def armNum(self, cmd):
        """Return the correct arm number: 1, 2, or 4.

        For the red cryostats, we have two arm numbers: 2 for low res,
        and 4 for medium res. This number is used (only?) in the
        filename. Resolve which to use.

        We _want_ to use the dcbActor rexm keyword. But we also allow
        manually overriding that from the self.actor.grating
        variable. That may only ever be used for code testing.

        """

        if self.actor.ids.arm != 'r':
            return self.actor.ids.armNum
        if hasattr(self.actor, 'grating') and self.actor.grating != 'real':
            arms = dict(low=2, med=4)
            cmd.warn(f'text="using fake grating position {self.actor.grating}"')
            return arms[self.actor.grating]

        try:
            rexm = self.actor.enuModel.keyVarDict['rexm'].getValue()
        except Exception as e:
            self.logger.warn('failed to get enu grating position: %s', e)
            cmd.warn('text="failed to get enu grating position: using low"')
            return 2

        try:
            # ENU uses "mid", which I think should be changed.
            arms = dict(low=2, mid=4, med=4)
            return arms[rexm]
        except KeyError:
            cmd.warn(f'text="enu grating position invalid ({rexm}), using low for filename"')
            return 2

    def arm(self, cmd):
        """Return the correct arm: 'b', 'r', 'm', 'n'.

        For the red cryostats, we have two arms: 'r' for low res,
        and 'm' for medium res. See .armNum() for details on how this is resolved.

        """
        arms = {1:'b', 2:'r', 3:'n', 4:'m'}
        armNum = self.armNum(cmd)
        return arms[armNum]

    def findCard(self, cards, cardName):
        for c_i, c in enumerate(cards):
            if c.name == cardName:
                return c_i
        return -1

    def removeCard(self, cards, cardName):
        idx = self.findCard(cards, cardName)
        if idx >= 0:
            cards.pop(idx)

        return cards

    def getLightSource(self, cmd):
        """Return our lightsource (pfi, sunss, dcb, dcb2). """

        sm = self.actor.ids.specNum
        try:
            spsModel = self.actor.models['sps'].keyVarDict
            lightSource = spsModel[f'sm{sm}LightSource'].getValue()
        except Exception as e:
            cmd.warn('text="failed to fetch lightsource card!!! %s"' % (e))
            lightSource = "unknown"

        return lightSource.lower()

    def getImageCards(self, cmd=None):
        """Return the FITS cards for the image HDU, WCS, basically.

        Return the required Subaru cards plus a pixel-pixel WCS, per INSTRM-578.
        Sneak in the semi-standard INHERIT.
        """

        allCards = []
        allCards.append(dict(name='INHERIT', value=True, comment='Recommend using PHDU cards'))
        allCards.append(dict(name='BUNIT', value="ADU", comment='Pixel units for rescaled data'))
        allCards.append(dict(name='BLANK', value=-32768, comment='Unscaled value used for invalid pixels'))
        allCards.append(dict(name='BIN-FCT1', value=1, comment='X-axis binning'))
        allCards.append(dict(name='BIN-FCT2', value=1, comment='Y-axis binning'))
        allCards.extend(wcs.pixelWcsCards())

        return allCards

    def getSpectroCards(self, cmd):
        """Return the Subaru-specific spectroscopy cards.

        See INSTRM-1022 and INSTRM-578
        """

        cards = []
        try:
            arm = self.arm(cmd)
            cards = getSpsSpectroCards(arm)
        except Exception as e:
            cmd.warn('text="failed to fetch Subaru spectro cards: %s"' % (e))

        return cards

    def getPfsDesignCards(self, cmd):
        """Return the pfsDesign-associated cards.

        Knows about PFI, DCB and SuNSS cards. Uses the sps.lightSources key
        to tell us which to use.

        """

        cards = []

        lightSource = self.getLightSource(cmd)
        if lightSource == 'sunss':
            designId = 0xdeadbeef
            objectCard = 'SuNSS'
        elif lightSource == 'pfi':
            try:
                model = self.actor.models['iic'].keyVarDict
                designId = model['designId'].getValue()
            except Exception as e:
                cmd.warn(f'text="failed to get designId for {lightSource}: {e}"')
                designId = 9998
            # Let the gen2 keyword stay
            objectCard = None
        elif lightSource in {'dcb', 'dcb2'}:
            try:
                model = self.actor.models[lightSource].keyVarDict
                designId = model['designId'].getValue()
            except Exception as e:
                cmd.warn(f'text="failed to get designId for {lightSource}: {e}"')
                designId = 9998
            objectCard = f'{lightSource}'
        else:
            cmd.warn(f'text="unknown lightsource ({lightSource}) for a designId')
            designId = 9999
            objectCard = 'unknown'

        if objectCard is not None:
            cards.append(dict(name='OBJECT', value=objectCard, comment='Internal id for this light source'))
        cards.append(dict(name='W_PFDSGN', value=int(designId), comment=f'pfsDesign, from {lightSource}'))
        cards.append(dict(name='W_LGTSRC', value=str(lightSource), comment='Light source for this module'))
        return cards

    def getBeamConfigCards(self, cmd, visit):
        """Generate header cards and synthetic date for the state of the beam-affecting hardware.

        Current rules:
         - light source is whatever single source sps has for this spectro module.
         - beamConfigDate = max(fpaConfigDate, hexapodConfigDate)
         - if red, also use gratingMoved date
         - if either DCB is connected, also use dcbConfigDate

        Generate all cards appropriate for this cryostat and configuration.
        """

        anyBad = False
        dcbDate = 9998.0
        fpaDate = 9998.0
        hexapodDate = 9998.0
        gratingDate = 9998.0

        lightSource = self.getLightSource(cmd)
        haveDcb = lightSource in {'dcb', 'dcb2'}
        if haveDcb:
            try:
                dcbModel = self.actor.models[lightSource]
                dcbDate = dcbModel.keyVarDict['dcbConfigDate'].getValue()
            except Exception as e:
                cmd.warn(f'text="failed to get {lightSource} beam dates: {e}"')
                anyBad = True

        try:
            xcuModel = self.actor.xcuModel
            fpaDate = xcuModel.keyVarDict['fpaMoved'].getValue()
        except Exception as e:
            cmd.warn(f'text="failed to get xcu beam dates: {e}"')
            anyBad = True

        try:
            enuModel = self.actor.enuModel
            hexapodDate = enuModel.keyVarDict['hexapodMoved'].getValue()
        except Exception as e:
            cmd.warn(f'text="failed to get enu hexapod beam date: {e}"')
            anyBad = True

        isRed = self.arm(cmd) in {'r', 'm'}
        if isRed:
            try:
                enuModel = self.actor.enuModel
                gratingDate = enuModel.keyVarDict['gratingMoved'].getValue()
            except Exception as e:
                cmd.warn(f'text="failed to get enu grating beam dates: {e}"')
                anyBad = True

        if anyBad:
            beamConfigDate = 9998.0
            cmd.warn(f'beamConfigDate={visit},{beamConfigDate:0.6f}')
        else:
            beamConfigDate = max(fpaDate, hexapodDate)

            if isRed:
                beamConfigDate = max(beamConfigDate, gratingDate)

            if haveDcb:
                beamConfigDate = max(beamConfigDate, dcbDate)

            cmd.inform(f'beamConfigDate={visit},{beamConfigDate:0.6f}')

        allCards = []
        allCards.append(dict(name='COMMENT', value='################################ Beam configuration'))
        allCards.append(dict(name='W_SBEMDT', value=float(beamConfigDate), comment='[day] Beam configuration time'))
        allCards.append(dict(name='W_SFPADT', value=float(fpaDate), comment='[day] Last FPA move time'))
        allCards.append(dict(name='W_SHEXDT', value=float(hexapodDate), comment='[day] Last hexapod move time'))
        if haveDcb:
            allCards.append(dict(name='W_SDCBDT', value=float(dcbDate), comment='[day] Last DCB configuration time'))
        if isRed:
            allCards.append(dict(name='W_SGRTDT', value=float(gratingDate), comment='[day] Last grating move time'))

        return allCards

    def getMhsCards(self, cmd):
        """ Gather FITS cards from all *other* actors we are interested in. """

        modelNames = list(self.actor.models.keys())
        modelNames.remove(self.actor.name)
        cmd.debug(f'text="provisionally fetching MHS cards from {modelNames}"')

        # Lamps are picked up more carefully: we need to select exactly one of these
        for lampsName in 'pfilamps', 'dcb', 'dcb2':
            if lampsName in modelNames:
                modelNames.remove(lampsName)

        cmd.debug(f'text="fetching MHS cards from {modelNames}"')
        cards = fitsUtils.gatherHeaderCards(cmd, self.actor,
                                            modelNames=modelNames,shortNames=True)
        cmd.debug('text="fetched %d MHS cards..."' % (len(cards)))

        return cards

    def getStartInstCards(self, cmd):
        """Gather cards at the start of integration."""

        cards = []

        return cards

    def getEndInstCards(self, cmd):
        """Gather cards at the end of integration. Calibration lamps, etc. """

        cards = []

        try:
            lightSource = self.getLightSource(cmd)
            if lightSource == 'pfi':
                modelNames = ['pfilamps']
            else:
                modelNames = [lightSource]
            cmd.debug(f'text="fetching ending MHS cards from {modelNames}"')
            cards = fitsUtils.gatherHeaderCards(cmd, self.actor,
                                                modelNames=modelNames,shortNames=True)
            cmd.debug('text="fetched %d ending MHS cards..."' % (len(cards)))
        except Exception as e:
            cmd.warn(f'text="failed to fetch ending cards: {e}"')

        return cards

    def finishHeaderKeys(self, cmd, visit, timeCards):
        """ Finish the header. Called just before readout starts. Must not block! """

        if cmd is None:
            cmd = self.cmd

        gain = 9999.0
        detectorId = self.actor.ids.camName
        try:
            xcuModel = self.actor.xcuModel
            detectorTemp = xcuModel.keyVarDict['temps'].getValue()[-1]
        except Exception as e:
            cmd.warn(f'text="failed to get detector temp for Subaru: {e}"')
            detectorTemp = 9998.0

        exptype = self.exptype.upper()
        if exptype == 'ARC':
            exptype = 'COMPARISON'

        try:
            detId = self.actor.ids.idDict['fpaId']
        except Exception as e:
            cmd.warn(f'text="failed to get FPA id: {e}"')
            detId = -1

        beamConfigCards = self.getBeamConfigCards(cmd, visit)
        spectroCards = self.getSpectroCards(cmd)
        designCards = self.getPfsDesignCards(cmd)
        endCards = self.getEndInstCards(cmd)
        mhsCards = self.getMhsCards(cmd)

        # We might be overriding the Subaru/gen2 OBJECT.
        if self.findCard(designCards, 'OBJECT') >= 0:
            self.removeCard(mhsCards, 'OBJECT')

        allCards = []
        allCards.append(dict(name='DATA-TYP', value=exptype, comment='Subaru-style exposure type'))
        allCards.append(dict(name='FRAMEID', value=f'PFSA{visit:06d}00',
                             comment='Sequence number in archive'))
        allCards.append(dict(name='EXP-ID', value=f'PFSE00{visit:06d}',
                             comment='PFS exposure visit number'))
        allCards.append(dict(name='DETECTOR', value=detectorId, comment='Name of the detector/CCD'))
        allCards.append(dict(name='GAIN', value=gain, comment='[e-/ADU] AD conversion factor'))
        allCards.append(dict(name='DET-TMP', value=float(detectorTemp), comment='[K] Detector temperature'))
        allCards.append(dict(name='DET-ID', value=detId, comment='Subaru/DRP FPA ID for this module and arm'))
        allCards.extend(spectroCards)
        allCards.append(dict(name='COMMENT', value='################################ PFS main IDs'))

        allCards.append(dict(name='W_VISIT', value=int(visit), comment='PFS exposure visit number'))
        allCards.append(dict(name='W_ARM', value=self.armNum(cmd),
                             comment='Spectrograph arm 1=b, 2=r, 3=n, 4=medRed'))
        allCards.append(dict(name='W_SPMOD', value=self.actor.ids.specNum,
                             comment='Spectrograph module. 1-4 at Subaru'))
        allCards.append(dict(name='W_SITE', value=self.actor.ids.site,
                             comment='PFS DAQ location: Subaru, Jhu, Lam, Asiaa'))
        allCards.extend(designCards)

        allCards.append(dict(name='COMMENT', value='################################ Time cards'))
        allCards.extend(timeCards)

        allCards.extend(mhsCards)
        allCards.extend(endCards)
        # allCards.extend(self.headerCards)
        allCards.extend(beamConfigCards)

        keepCards = []
        for c_i, c in enumerate(allCards):
            if not isinstance(c['value'], (int, bool, float, str)):
                cmd.warn(f'text="bad card: {c}')
            else:
                keepCards.append(c)
        allCards = keepCards

        return allCards
