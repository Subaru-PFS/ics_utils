import logging
import pathlib

import fitsio
import numpy as np

logger = logging.getLogger('hxramp')

class HxRamp(object):
    nrows = 4096
    ncols = 4096

    def __init__(self, fitsOrPath, logLevel=None):
        """Basic H4 ramp object, wrapping a PFS PFxB FITS file.

        Used to be generic H2 and H4, but given IRP on the H4s I'm giving up on that.

        Args
        ----
        fitsOrPath : `fitsio.FITS` or path-like.
            The path to open as a FITS file or an existing FITS object to use.
        """
        self.logger = logging.getLogger('hxramp')
        if logLevel is not None:
            self.logger.setLevel(logLevel)

        if not isinstance(fitsOrPath, fitsio.FITS):
            p = pathlib.Path(fitsOrPath)
            self.cam = 'n' + p.stem[-2]
            fitsOrPath = fitsio.FITS(fitsOrPath)
        self.fits = fitsOrPath
        self.phdu = self.header()

        self.calcBasics()

    def __str__(self):
        return (f"HxRamp(filename={self.fits._filename}, nReads={self.nreads}, "
                f"interleave={self.interleaveRatio})")

    def __del__(self):
        """ fitsio caches open files, so try to close when we know we want to."""
        if self.fits is not None:
            self.fits.close()
            self.fits = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        """ fitsio caches open files, so try to close when we know we want to."""
        if self.fits is not None:
            self.fits.close()
            self.fits = None
        return True

    def calcBasics(self):
        """Figure out some basic properties of the ramp, using the first read or header.

        Specifically, get:
        - nreads
        - hchan
        - interleave{Ratio, Offset}
        - frameTime, rampTime
        """

        # Disgusting: figure out how many reads we have by looking at last HDU's EXTNAME.
        #
        lastHdr = self.fits[-1].read_header()
        lastName = lastHdr['extname']
        _, num = lastName.split('_')
        num = int(num)
        self.nreads = num

        read0 = self.dataN(0)
        self.height, self.width = read0.shape
        self.frameTime = self.phdu['W_H4FRMT']
        self.rampTime = self.phdu['EXPTIME']  # This will be wrong when called by SPS

        try:
            self.interleaveRatio = self.phdu['W_H4IRPN']
            self.interleaveOffset = self.phdu['W_H4IRPO']
        except KeyError:
            self.logger.warn('header does not have interleave keys, using data and guessing offset.')

            irp0 = self.irpN(0)

            if irp0 == 0:
                self.interleaveRatio = 0
            else:
                self.interleaveRatio = read0.shape[1] // irp0.shape[1]
            self.interleaveOffset = self.interleaveRatio

        try:
            self.nchan = self.phdu['W_H4NCHN']
        except KeyError:
            self.logger.warn('header does not have nchannels key, using 32.')
            self.nchan = 32

    def _readIdxToAbsoluteIdx(self, n):
        """Convert possibly negative 0-indexed ramp read index into positive 0-indexed read"""
        nums = range(0, self.nreads)
        return nums[n]

    def _readIdxToFITSIdx(self, n):
        """Convert possibly negative 0-indexed ramp read index into positive 1-indexed HDU"""
        return self._readIdxToAbsoluteIdx(n) + 1

    def header(self, readNum=None):
        if readNum is None:
            return self.fits[0].read_header()
        else:
            idx = self._readIdxToFITSIdx(readNum)
            return self.fits[f'IMAGE_{idx}'].read_header()

    def hduByName(self, hduName):
        return self.fits[hduName].read()

    def dataN(self, n):
        """Return the data plane for the n-th read.

        Args
        ----
        n : `int`
          0-indexed read number

        Returns
        -------
        im : np.uint16 image
        """
        n = self._readIdxToFITSIdx(n)
        extname = f'IMAGE_{n}'
        return self.fits[extname].read()

    def irpN(self, n, raw=False, refPix4=False):
        """Return the reference plane for the n-th read.

        If the IRP HDU is empty we did not acquire using IRP. So return 0.

        Does not interpolate N:1 IRP planes to full size images.

        Args
        ----
        n : `int`
          0-indexed read number
        raw : `bool`
          If True, do not process/interpolate the raw IRP image.
        refPix4 : `bool`
          If True, return the `refpix4` image, based on the border pixels.
          Very unlikely to be what you want.

        Returns
        -------
        im : np.uint16 image, or 0 if there is none.

        """

        if refPix4:
            dataImage = self.dataN(n).astype('f4')
            corrImage, *_ = refPixel4(dataImage)
            return corrImage - dataImage  # Meh. Should have refPixel4 return the full correction image?

        n = self._readIdxToFITSIdx(n)
        extname = f'REF_{n}'
        try:
            irpImage = self.fits[extname].read()
        except:
            irpImage = None

        if irpImage is None or np.isscalar(irpImage) or irpImage.shape == (1,1):
            return np.uint16(0)

        if not raw:
            irpImage0 = irpImage
            irpImage = constructFullIrp(irpImage, self.nchan,
                                        refPix=self.interleaveOffset)
        else:
            return irpImage

    def readN(self, n, doCorrect=True, refPixel4=False):
        """Return the IRP-corrected image for the n-th read.

        Note that data - ref is often negative, so we convert to float32 here.

        Args
        ----
        n : `int`
          0-indexed read number
        doCorrect : `bool`
          Apply reference pixel correction.
        refPixel4 : `bool`
          If True, correct using the border reference pixels

        Returns
        -------
        im : np.float32 image
        """

        data = self.dataN(n).astype('f4')

        if self.interleaveRatio > 0 and not refPixel4:
            if doCorrect:
                data = data - self.irpN(n).astype('f4')
        else:
            if doCorrect:
                corrected, *_ = refPixel4(data)  # Beware: refPixel4 could be better.
                data = corrected
        return data

    def cdsN(self, r0=0, r1=-1, refPixel4=False):
        """Return the CDS image between two reads.

        This is the most common way to get an quick image from an IRP H4 ramp, but is
        not the *right* way to do it.
        See .readStack() to get closer to that.

        Args
        ----
        r0 : `int`
          0-indexed read number of the 1st read
        r1 : `int`
          0-indexed read number of the 2st read
        refPixel4 : `bool`
          If True, correct using the border reference pixels

        Returns
        -------
        im : np.float32 image
        """
        return self.readN(r1, refPixel4=refPixel4) - self.readN(r0, refPixel4=refPixel4)

    cds = cdsN

    def dataStack(self, r0=0, r1=-1, dtype='u2'):
        """Return all the data frames, in a single 3d stack.

        Args
        ----
        r0 : `int`
          The 0-indexed read to start from.
        r1 : `int`
          The 0-indexed read to end with
        dtype : `str`
          If set and not "u2", the dtype to coerce to.

        Returns
        -------
        stack : the 3-d stack, with axis 0 being the reads.
        """

        r0 = self._readIdxToAbsoluteIdx(r0)
        r1 = self._readIdxToAbsoluteIdx(r1)
        nreads = r1 - r0 + 1

        stack = np.empty(shape=(nreads,self.ncols,self.nrows), dtype=dtype)
        for r_i in range(r0, r1+1):
            read = self.dataN(r_i)
            stack[r_i,:,:] = read

        return stack

    def irpStack(self, r0=0, r1=-1, dtype='u2', raw=False, refPix4=False):
        """Return all the reference frames, in a single 3d stack.

        Args
        ----
        r0 : `int`
          The 0-indexed read to start from.
        r1 : `int`
          The 0-indexed read to end with
        dtype : `str`
          If set and not "u2", the dtype to coerce to.
        raw : `bool`
          If True, do not interpolate/proceess the reference images
        refPix4 : `bool`
          If True, return the refPixel4 corrections.

        Returns
        -------
        stack : the 3-d stack, with axis 0 being the reads.
        """

        if refPix4 and dtype != 'f4':
            raise ValueError('irpRamps using refPixel4 cannot be unsigned shorts')
        r0 = self._readIdxToAbsoluteIdx(r0)
        r1 = self._readIdxToAbsoluteIdx(r1)
        nreads = r1 - r0 + 1

        stack = np.empty(shape=(nreads,self.ncols,self.nrows), dtype=dtype)
        for r_i in range(r0, r1+1):
            read = self.irpN(r_i, raw=raw, refPix4=refPix4)
            stack[r_i,:,:] = read

        return stack

    def readStack(self, r0=0, r1=-1, refPixel4=False):
        """Return all the ref-corrected frames, in a single 3d stack.

        Note that there will be one fewer reads than in the data: r0
        is subtracted from all later reads.

        This is probably close to where proper reductions will start
        from: all the reference-corrected reads in a single
        stack. Easy to pick up CRs, or to apply linearity,
        etc. corrections.

        If we were in space, could then simply fit lines through the
        pixels (or do something as trivial as
        np.mean(np.diff(axis=0)).

        Args
        ----
        r0 : `int`
          The 0-indexed read to start from.
        r1 : `int`
          The 0-indexed read to end with
        refPixel4 : `bool`
          If True, correct using the border reference pixels

        Returns
        -------
        stack : the 3-d stack, with axis 0 being the reads. Always 'f4'.

        """

        r0 = self._readIdxToAbsoluteIdx(r0)
        r1 = self._readIdxToAbsoluteIdx(r1)
        nreads = r1 - r0 + 1

        stack = np.empty(shape=(nreads,self.ncols,self.nrows), dtype='f4')
        for r_i in range(r0, r1+1):
            read1 = self.readN(r_i, refPixel4=refPixel4)
            stack[r_i-1,:,:] = read1

        return stack

    def cdsStack(self, r0=0, r1=-1, refPixel4=False):
        """Return all the CDS frames, in a single 3d stack.

        Note that there will be one fewer reads than in the data: r0
        is subtracted from all later reads.

        Args
        ----
        r0 : `int`
          The 0-indexed read to subtract from subsequent reads.
        r1 : `int`
          The 0-indexed read to end with
        refPixel4 : `bool`
          If True, correct using the border reference pixels

        Returns
        -------
        stack : the 3-d stack, with axis 0 being the reads. Always 'f4'.

        """

        r0 = self._readIdxToAbsoluteIdx(r0)
        r1 = self._readIdxToAbsoluteIdx(r1)
        nreads = r1 - r0

        stack = np.empty(shape=(nreads,self.ncols,self.nrows), dtype='f4')
        read0 = self.readN(r0, refPixel4=refPixel4)
        for r_i in range(r0+1, r1+1):
            read = self.readN(r_i, refPixel4=refPixel4)
            stack[r_i-1,:,:] = read - read0

        return stack

def rebin(arr, factors):
    """Bin 2d-array by the given factors

    Parameters
    ----------
    arr : `np.ndarray`
        2d array
    factors : pair-like
        factor by which to bin the `arr` dimensions.

    Returns
    -------
    `np.ndarray`
        smaller array
    """
    factors =  np.array(factors)
    new_shape = np.array(arr.shape) // factors

    shape = (new_shape[0], factors[0],
             new_shape[1], factors[1])
    return arr.reshape(shape).mean([-1, 1])

def interpolateChannelIrp(rawChan, refRatio, refOffset, doFlip=True):
    """Given a single channel's IRP image and the IRP geometry, return a full-size reference image for the channel

    Args
    ----
    rawChan : array
     The raw IRP channel, with the columns possibly subsampled by an integer factor.
    refRatio : int
     The factor by which the reference pixels are subsampled.
    refOffset : int
     The position of the reference pixel w.r.t. the associated science pixels.
    doFlip : bool
     Whether the temporal order of the columns is right-to-left.

    Returns
    -------
    im : the interpolated reference pixel channel.

    We do not yet know how to interpolate, so simply repeat the pixel refRatio times.

    """
    irpHeight, irpWidth = rawChan.shape
    refChan = np.empty(shape=(irpHeight, irpWidth * refRatio), dtype=rawChan.dtype)

    # For now, use the per-row median
    # refChan[:,:] = np.median(rawChan, axis=1)[:,None]
    # return refChan

    # Or repeat
    if doFlip:
        rawChan = rawChan[:, ::-1]

    for i in range(0, refRatio):
        refChan[:, i::refRatio] = rawChan

    if doFlip:
        refChan = refChan[:, ::-1]

    return refChan

def constructFullIrp(rawIrp, nChannel=32, refPix=None, oddEven=True):
    """Given an IRP image, return fullsize IRP image.

    Args
    ----
    rawImg : ndarray
      A raw read from the ASIC, possibly with interleaved reference pixels.
    nChannel : `int`
      The number of readout channels from the H4
    refPix : `int`
      The number of data pixels to read before a reference pixel.
    oddEven : `bool`
      Whether readout direction flips between pairs of amps.
      With the current Markus Loose firmware, that is always True.

    Returns
    -------
    img : full 4096x4096 image.

    - The detector was read out in nChannel channels, usually 32, but possibly 16, 4, or 1.

    - If oddEven is set (the default), the read order from the
      detector of pairs of channels is --> <--. The ASIC "corrects"
      that order so that the image always "looks" right: the
      column-order of the image is spatially, not temporally correct.

    - the N:1 ratio of science to reference pixels is deduced from the size of the image.

    - refPix tells us the position of the reference pixel within the N
      science pixels. It must be >= 1 (there must be at least one
      science pixel before the reference pixel). The ASIC default is
      for it to be the last pixel in the group.

    Bugs
    ----
    oddEven should be {0,1},{0,1} to let us cover all the possibilities.
    """

    logger = logging.getLogger('constructIRP')
    logger.setLevel(logging.INFO)

    h4Width = 4096
    height, width = rawIrp.shape

    # If we are a full frame, no interpolation is necessary.
    if False and width == h4Width:
        return rawIrp

    dataChanWidth = h4Width // nChannel
    refChanWidth = width // nChannel
    refRatio = h4Width // width
    refSkip = refRatio + 1

    if refPix is None:
        refPix = refRatio
    logger.debug(f"constructIRP {rawIrp.shape} {nChannel} {dataChanWidth} {refChanWidth} {refRatio} {refPix}")

    refChans = []
    for c_i in range(nChannel):
        rawChan = rawIrp[:, c_i*refChanWidth:(c_i+1)*refChanWidth]
        doFlip = oddEven and c_i%2 == 1

        # This is where we would intelligently interpolate.
        refChan = interpolateChannelIrp(rawChan, refRatio, refPix, doFlip)
        refChans.append(refChan)

    refImg = np.hstack(refChans)

    logger.debug(f"constructIRP {rawIrp.shape} {refImg.shape}")

    return refImg

def splitIRP(rawImg, nChannel=32, refPix=None, oddEven=True):
    """Given a single read from the DAQ, return the separated data and the reference images.

    Args
    ----
    rawImg : ndarray
      A raw read from the ASIC, possibly with interleaved reference pixels.
    nChannel : `int`
      The number of readout channels from the H4
    refPix : `int`
      The number of data pixels to read before a reference pixel.
    oddEven : `bool`
      Whether readout direction flips between pairs of amps.

    The image is assumed to be a full-width 4k read: IRP reads can only be full width.

    Bugs
    ====
    oddEven should be {0,1},{0,1} to let us cover all the possibilities.
    """

    logger = logging.getLogger('splitIRP')

    h4Width = 4096
    height, width = rawImg.shape

    # Can be no IRP pixels
    if width <= h4Width:
        return rawImg, None

    dataWidth = h4Width
    dataChanWidth = dataWidth // nChannel
    rawChanWidth = width // nChannel
    refChanWidth = rawChanWidth - dataChanWidth
    refRatio = dataChanWidth // refChanWidth
    refSkip = refRatio + 1

    if refPix is None:
        refPix = refRatio
    logger.debug(f"splitIRP {rawImg.shape} {nChannel} {dataChanWidth} "
                 f"{rawChanWidth} {refChanWidth} {refRatio} {refPix}")

    refChans = []
    dataChans = []
    for c_i in range(nChannel):
        rawChan = rawImg[:, c_i*rawChanWidth:(c_i+1)*rawChanWidth]
        doFlip = oddEven and c_i%2 == 1

        if doFlip:
            rawChan = rawChan[:, ::-1]
        refChan = rawChan[:, refPix::refSkip]

        dataChan = np.zeros(shape=(height, dataChanWidth), dtype='u2')
        dataPix = 0
        for i in range(refRatio+1):
            # Do not copy over reference pixels, wherever they may be.
            if i == refPix:
                continue
            dataChan[:, dataPix::refRatio] = rawChan[:, i::refSkip]
            dataPix += 1

        if doFlip:
            refChan = refChan[:, ::-1]
            dataChan = dataChan[:, ::-1]

        refChans.append(refChan)
        dataChans.append(dataChan)

    refImg = np.hstack(refChans)
    dataImg = np.hstack(dataChans)

    logger.debug(f"splitIRP {rawImg.shape} {dataImg.shape} {refImg.shape}")

    return dataImg, refImg

def ampSlices(im, ampN, nAmps=32):
    height, width = im.shape
    ampWidth = width//nAmps
    slices = slice(height), slice(ampN*ampWidth, (ampN+1)*ampWidth)

    return slices

def refPixel4(im, doRows=True, doCols=True, nCols=4, nRows=4, colWindow=4):
    """ Apply Teledyne's 'refPixel4' scheme.

    Step 1:
       For each amp, average all 8 top&bottom rows to one number.
       Subtract that from the amp.

    Step 2:
        Take a 9-row running average of the left&right rows.
        Subtract that from each row.

    The 8 rows/columns are wildly different, but we think they are stable. In
    any case we are just here to duplicate the Teledyne logic.
    """

    im = im.astype('f4')
    corrImage = np.zeros_like(im)
    imHeight, imWidth = im.shape

    rowRefs = np.zeros((nRows*2, imHeight), dtype='f4')
    rowRefs[0:nRows,:] = im[4-nRows:4,:]
    rowRefs[nRows:,:] = im[-nRows:,:]

    ampRefMeans = []
    for amp_i in range(32):
        slices = ampSlices(im, amp_i)
        ampImage = im[slices].copy()
        ampRefMean = (ampImage[4-nRows:4,:].mean()
                      + ampImage[-nRows:,:].mean()) / 2
        ampRefMeans.append(ampRefMean)
        ampImage -= ampRefMean
        corrImage[slices] = ampImage
    corr1Image = corrImage - im

    if not doRows:
        corrImage = im.copy()

    sideRefImage = np.ndarray((imHeight, nCols*2), dtype=im.dtype)
    sideRefImage[:, :nCols] = corrImage[:, 4-nCols:4]
    sideRefImage[:, -nCols:] = corrImage[:, -nCols:]
    sideCorr = np.zeros((imHeight,1))
    for row_i in range(colWindow, imHeight-colWindow+1):
        sideCorr[row_i] = sideRefImage[row_i-colWindow:row_i+colWindow,:].mean()

        if doCols:
            corrImage[row_i, :] -= sideCorr[row_i]

    return corrImage, ampRefMeans, corr1Image, rowRefs, sideRefImage, sideCorr

# Standard entry point for ICS butler
def load(path):
    return HxRamp(path)
