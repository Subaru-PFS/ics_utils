import logging
from multiprocessing import Process, Queue
from multiprocessing import shared_memory
import numpy as np
import os
import pathlib
import time
import threading

import fitsio

class FitsWriter(object):
    def __init__(self, inQ, outQ, doCompress=True, rampRoot=None):
        """Buffer FITS file writes.

        Args
        ----
        inQ : `Queue`
            Queue we receive commands on
        outQ : `Queue`
            Queue we report success/failure to.
        doCompress : `bool`
            If True, use RICE compression
        rampRoot : `path-like`
            If set, arrange to save physical ramp files in this directory

        Queue commands are:
        'create', path, header : create new FITS file, with given PHDU
        'write', hduId, extname, data, hdr : append new HDU to our file.
        'close' : finish out the current file. Rename to final name if we are writing to a scratch file.
        'shutdown': exits process after closing any open file.

        where:

        path : FITS pathname we eventually generate
        hduId : identifier we return when this HDU is written.
        extname : the FITS EXTNAME we identify this HDU with
        data : None or an image. We compress with RICE_1, so should be uint16
        header : a fitsio-compliant list of dicts
        """
        self.logger = logging.getLogger('fitsWriter')
        self.inQ = inQ
        self.outQ = outQ
        self.doCompress = doCompress
        self.rampRoot = rampRoot

        self.currentPath = None
        self.tempPath = None
        self.currentFits = None

    @classmethod
    def setupFitsWriter(cls, inQ, outQ, doCompress=True, rampRoot=None):
        """Convenience wrapper for Process creation."""
        fw  = cls(inQ, outQ, doCompress=doCompress, rampRoot=rampRoot)
        fw.run()

    def report(self, command, *, path=None, hduId=None, status=None, errorDetails=None):
        try:
            self.outQ.put(dict(command=command, status=status,
                               path=path, hduId=hduId,
                               errorDetails=errorDetails))
        except Exception as e:
            self.logger.warning(f'failed to send a command to self.outQ: {e}')

    def reportSuccess(self, command, *, path=None, hduId=None):
        self.report(command, path=path, hduId=hduId, status='OK', errorDetails=None)

    def reportFailure(self, command, *, path=None, hduId=None, errorDetails='ERROR'):
        self.report(command, path=path, hduId=hduId, status='ERROR', errorDetails=errorDetails)

    def _makePfsDir(self, dirpath):
        """Create a well-configured directory.

        Slightly over-tricky and innards-aware.

        If self.rampRoot is set, we want to save the actual ramp files
        into a separate directory (filesystem), and turn .dirpath
        into a symlink.

        Args:
        -----
        dirpath : `pathlib.Path`
           the final directory name we want.
           In the form: f'{root}/{date}/ramps'

        if self.rampRoot, then make f'{self.rampRoot}/{date}' and link to that.

        Args
        ----
        dirpath : `pathlib.Path`
          the name of the directory we need to create.
        """

        # Should work whether we are a directory or a symlink
        if dirpath.exists():
            return

        if self.rampRoot:
            # We want the following directories and links.
            #   /data/raw/$date/ramps/ -> /data/ramps/$date/
            #
            rawDateDir = dirpath.parent
            date = dirpath.parts[-2]
            realDir = pathlib.Path(self.rampRoot, date)

            realDir.mkdir(mode=0o755, parents=True, exist_ok=True)
            rawDateDir.mkdir(mode=0o755, parents=True, exist_ok=True)
            dirpath.symlink_to(realDir)
        else:
            dirpath.mkdir(mode=0o755, parents=True, exist_ok=True)

    def create(self, path, header, useTemp=True):
        """Create a new FITS file and generate the PHDU

        Parameters
        ----------
        path : path-like
            The full pathname of the FITS file
        header : fitsio card dict
            The PHDU cards
        useTemp : bool
            Whether to write to a temp file, then rename at end.
        """
        try:
            self.currentPath = path
            path = pathlib.Path(path)
            self._makePfsDir(path.parent)

            if useTemp:
                self.tempPath = pathlib.Path(path.parent, '.'+path.name)
                self.currentFits = fitsio.FITS(self.tempPath, mode='rw')
            else:
                self.tempPath = None
                self.currentFits = fitsio.FITS(self.currentPath, mode='rw')

        except Exception as e:
            self.logger.warning(f'failed to create {path}: {e}')
            self.reportFailure('CREATE', path=path,
                               errorDetails=f'ERROR creating {path}(tempPath={self.tempPath}): {e}')
            self.currentPath = None
            self.currentFits = None
            self.tempPath = None

        try:
            self.currentFits.write(None, header=header)
            self.reportSuccess('CREATE', path=self.currentPath)
        except Exception as e:
            self.logger.warning("failed to write PHDU to {self.currentFits}")
            self.reportFailure('CREATE', path=self.currentPath,
                               errorDetails='ERROR writing PHDU: {e}')

    @classmethod
    def fetchData(cls, keys):
        """Fetch transferred data from shared memory and free buffer.

        If we did not go though a shared memory array, simply return the keys argument.
        This routine is responsible for freeing the transfer buffer.

        Parameters
        ----------
        keys : `tuple`
             Data descriptor for fetching and reshaping:
             - shared memory segment name
             - numpy dtype of data
             - numpy shape of data

             Matches the tuple returned by the `shareData` method.

        Returns
        -------
        array : `np.ndarray`
            reshaped data array
        """
        try:
            name, dtype, shape = keys
        except ValueError:
            return keys

        shm = shared_memory.SharedMemory(name=name, create=False)
        _data = np.ndarray(dtype=dtype, shape=shape, buffer=shm.buf)
        data = _data.copy()

        shm.close()
        shm.unlink()
        del shm

        return data

    @classmethod
    def shareData(cls, data):
        """Copy data array to new shared memory buffer and hand off ownership.

        Parameters
        ----------
        data : `np.ndarray`
            A data array to transfer

        Returns
        -------
        descriptor : `tuple`
            See `fetchData`
        """
        shm = shared_memory.SharedMemory(name=None, create=True, size=data.nbytes)
        dataBuf = np.ndarray(dtype=data.dtype, shape=data.shape, buffer=shm.buf)
        dataBuf[:] = data[:]
        shm.close()

        return (shm.name, dataBuf.dtype.name, dataBuf.shape)

    def write(self, hduId, extname, data, header):
        """Append an image HDU to our file

        Parameters
        ----------
        hduId : object
            Whatever the main program sent to identify this HDU
        extname : `str``
            The FITS EXTNAME for this HDU
        data : None or `np.array`
            The image data. We always RICE_1 compress, so this should be uint16.
        header : fitsio-compliant cardlist
            The header cards for this HDU
        """

        try:
            compress = None if (data is None or not self.doCompress) else 'RICE_1'
            realData = self.fetchData(data)
            self.currentFits.write(realData, header=header, extname=extname, compress=compress)

            if data is not None:
                self.currentFits[-1].write_checksum()
            self.reportSuccess('WRITE', path=self.currentPath, hduId=hduId)
        except Exception as e:
            self.logger.warning(f'failed to write hdu {hduId} for {self.currentPath}: {e}')
            self.reportFailure('WRITE', path=self.currentPath, hduId=hduId,
                               errorDetails=f'ERROR writing {hduId}: {e}')

    def amend(self, cards):
        """Amend the PHDU with some cards

        Parameters
        ----------
        header : fitsio-compliant cardlist
            The header cards for this HDU
        """

        try:
            phdu = self.currentFits[0]
            phdu.write_keys(cards)
            self.reportSuccess('AMEND', path=self.currentPath)
        except Exception as e:
            self.logger.warning(f'failed to update phdu for {self.currentPath}: {e}')
            self.reportFailure('AMEND', path=self.currentPath,
                               errorDetails=f'ERROR writing PHDU: {e}')

    def close(self):
        """Actually close out any open FITS file.

        If we are writing to a tempfile, rename it to the real name.
        """
        self.logger.info(f'closing {self.currentPath}')
        try:
            try:
                self.currentFits.close()
            except Exception as e:
                self.logger.warning(f'failed to close {self.currentFits}: {e}')
                # Keep going....

            if self.tempPath is not None:
                # Rename temp file to final name
                self.logger.info(f'renaming {self.tempPath} to {self.currentPath}')
                os.rename(self.tempPath, self.currentPath)
            self.reportSuccess('CLOSE', path=self.currentPath)
        except Exception as e:
            self.logger.warning(f'failed to close out {self.currentPath}: {e}')
            self.reportFailure('CLOSE', path=self.currentPath,
                               errorDetails=f'ERROR closing or renaming {self.currentPath}(temp={self.tempPath}): {e}')
        finally:
            self.currentFits = None
            self.currentPath = None
            self.tempPath = None

    def run(self):
        """Main process loop. Read and execute commands from .inQ """

        self.logger.info('starting loop...')
        while True:
            cmd = None
            try:
                self.logger.info('waiting for new command...')
                t0 = time.time()
                cmd = self.inQ.get()
                t1 = time.time()
                self.logger.info(f'new cmd (len={len(cmd)}), {t1-t0:0.4f}s')
                if isinstance(cmd, str):
                    self.logger.info(f'    {cmd}')
                if cmd == 'shutdown':
                    if self.currentFits is not None:
                        self.close()
                    self.logger.info(f'shutting down')
                    try:
                        self.reportSuccess('SHUTDOWN')
                    finally:
                        return
                if cmd == 'close':
                    self.close()
                    continue

                cmd, *cmdArgs = cmd
                if cmd == 'create':
                    self.logger.info(f'new cmd {cmd} (len={len(cmdArgs)})')
                    path, header = cmdArgs
                    if path is None:
                        raise ValueError("cannot open file without a path")
                    self.create(path, header)
                elif cmd == 'write':
                    hduId, extname, data, header = cmdArgs
                    t0 = time.time()
                    self.write(hduId, extname, data, header)
                    t1 = time.time()
                    self.logger.info(f'fitsio HDU write of {self.currentPath} {hduId} took {t1-t0:0.4f}s')
                elif cmd == 'amend':
                    cards, = cmdArgs
                    t0 = time.time()
                    self.amend(cards)
                    t1 = time.time()
                    self.logger.info(f'fitsio amending of {self.currentPath} PHDU took {t1-t0:0.4f}s')
                else:
                    raise ValueError(f'unknown command: {cmd} {cmdArgs}')
            except Exception as e:
                self.logger.warning(f'fitsWriter command {repr(cmd)} made no sense: {e}')

                self.reportFailure('PARSING', errorDetails=f'ERROR parsing {cmd}: {e}')
                return

class FitsCatcher(threading.Thread):
    def __init__(self, caller, replyQ, name=None):
        """Run a thread to listen for progress messages from FitsWriter, forward to an interested object

        Parameters
        ----------
        caller : `object`
            object to sed progress messages to.
        replyQ : `Queue`
            where we read FitsWriter progress messages from.
        """
        if name is None:
            name = self.__class__.__name__
        threading.Thread.__init__(self, target=self.loop, name=name)
        self.logger = logging.getLogger(name)
        self.caller = caller
        self.replyQ = replyQ

    def setCaller(self, newCaller):
        self.caller = newCaller

    def loop(self):
        """Listen for progress reports from FitsWriter, forward to exposure object.

        Messages are dictionaries, all with a `command` key which we simply use to dispatch
        to the exposure owner. The message dictionaries have other keys to help processing:
        - status: "OK", or "ERROR".
        - path: the FITS pathname
        - hduId: the HDU identifier which the exposure owner sent to the FitsWriter
        - errorDetails: if status is not OK, some hopefully informative string.
        """
        while True:
            reply = self.replyQ.get()
            self.logger.info(f'reply: {reply}')
            cmd = reply['command']
            if cmd == 'CLOSE':
                self.caller.closedFits(reply)
            elif cmd == 'WRITE':
                self.caller.wroteHdu(reply)
            elif cmd == 'AMEND':
                self.caller.amendedPHDU(reply)
            elif cmd == 'CREATE':
                self.caller.createdFits(reply)
            elif cmd == 'SHUTDOWN' or cmd == 'ENDCMD':
                return
            else:
                self.logger.warning(f'unknown fits writer reply: {reply}')

    def end(self):
        self.replyQ.put(dict(command='ENDCMD'))

class FitsBuffer(object):
    def __init__(self, doCompress=True, rampRoot=None):
        """Create a process to write FITS files """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cmdQ = Queue()
        self.replyQ = Queue()
        self.catcher = None
        self._p = Process(target=FitsWriter.setupFitsWriter,
                          args=(self.cmdQ, self.replyQ),
                          kwargs=dict(doCompress=doCompress, rampRoot=rampRoot))
        self.logger.info(f'starting FitsWriter {self._p}')
        self._p.start()

    def createFile(self, caller, path, header):
        """Create new FITS file.

        We actually create a scratch FITS file in the same directory as the final file.

        Parameters
        ----------
        caller : `object`
            Object we want to report back to, for progress made to this FITS file. Note
            that the caller can change per-FITS file, which lets us report back to the
            right command.
        path : path-like
            The full pathname of the FITS file we want to create.
        header : fitsio-compliant header
            The cards to create the PHDU with
        """

        if path is None:
            raise ValueError("must provide a pathname to create a FITS file.")
        if self.catcher is None:
            self.logger.info('creating FitsCatcher thread')
            self.catcher = FitsCatcher(caller, self.replyQ)
            self.catcher.start()
        else:
            self.catcher.setCaller(caller)

        self.cmdQ.put(('create', path, header))

    def addHdu(self, data, header, hduId=1, extname='IMAGE'):
        """Add a real image HDU

        Parameters
        ----------
        data : `np.ndarray`
            The image data. Expected to be uint16 since we always RICE_1 compress.
        header : fitsio-compilant cards
            The header for this HDU
        hduId : `int`
            Some identifier for the calling object.
        extname : `str`
            What to use as the EXTNAME
        """
        data = FitsWriter.shareData(data=data)
        self.cmdQ.put(('write', hduId, extname, data, header))

    def amendPHDU(self, cards):
        """Amend the PHDU with the given cards.

        God save you if the PDU needs to be extended.
        """
        self.cmdQ.put(('amend', cards))

    def finishFile(self):
        """Finalize our FITS file.

        We close the fitsio object, and rename the scratch file to the final proper filename.
        """
        self.cmdQ.put(('close'))

    def shutdown(self):
        """Kill our process, etc. """
        self.cmdQ.put('shutdown')

class TestCatcher(object):
    def __init__(self, name='testCatcher'):
        """Handle and report on all messsages coming from a FitsCatcher.

        The real object would report progess keywords to MHS keywords, and do _something_ about
        errors. Not sure what it would do.

        Parameters
        ----------
        name : str, optional
            some identifying name for log output, by default 'testCatcher'
        """
        self.logger = logging.getLogger('testCatcher')
        self.logger.setLevel(logging.INFO)
        self.name = name
        self.logger.info(f'created new TestCatcher(name={name})')

    def createdFits(self, reply):
        self.logger.info(f'{self.name} createdFits: {reply}')
    def wroteHdu(self, reply):
        self.logger.info(f'{self.name} wroteHdu: {reply}')
    def amendedPHDU(self, reply):
        self.logger.info(f'{self.name} amendedPHDU: {reply}')
    def closedFits(self, reply):
        self.logger.info(f'{self.name} closedFits: {reply}')
    def fitsFailure(self, reply):
        self.logger.warning(f'{self.name} fitsFailure: {reply}')

def testWrites(nfiles=5, nread=1, root='/tmp/testFits'):
    buffer = FitsBuffer()

    def makeCard(name, value, comment='no comment'):
        return dict(name=name, value=value, comment=comment)

    for fn_i in range(nfiles):
        fn = pathlib.Path(root) / f'test{fn_i}.fits'
        catcher = TestCatcher(name=f'catcher_{fn_i}')
        phdr = [makeCard('ISPHDU', True),
                makeCard('PATH', str(fn), 'the file path')]
        buffer.createFile(catcher, fn, phdr)

        for read_i in range(1, nread+1):
            im1 = np.arange(20, dtype='u2').reshape((5,4))
            im1 += 100*read_i
            hdr = [makeCard('ISPHDU', False),
                   makeCard('VAL', read_i, 'the read number')]
            extname = f'IMAGE_{read_i}' if nread > 1 else 'IMAGE'
            buffer.addHdu(im1, hdr, read_i, extname)
        buffer.finishFile()

    buffer.shutdown()

def testWriteOutput(nfiles=5, nread=1, root='/tmp/testFits'):
    for fn_i in range(nfiles):
        fn = pathlib.Path(root) / f'test{fn_i}.fits'

        try:
            ff = fitsio.FITS(fn)
        except Exception as e:
            logging.warning(f'failed to open {fn}: {e}')
            time.sleep(1)
            ff =  fitsio.FITS(fn)
        if len(ff) != nread + 1:
            raise RuntimeError(f"wrong number of reads in {fn}: {len(ff)} vs expected {nread}")
        phdu = ff[0].read_header()
        if phdu['PATH'] != str(fn):
            raise RuntimeError(f"expected correct PATH in {fn}")

        for read_i in range(1, nread+1):
            if nread == 1:
                hdu = ff['IMAGE']
            else:
                hdu = ff[f'IMAGE_{read_i}']
            hdr = hdu.read_header()
            data = hdu.read()

            if hdr['VAL'] != read_i:
                raise RuntimeError(f"expected VAL == {read_i} in {fn} HDU {read_i}, found {hdr['VAL']}")

            im1 = np.arange(20, dtype='u2').reshape((5,4))
            im1 += 100*read_i

            if np.any(data != im1):
                raise RuntimeError(f"data for {fn}, HDU {read_i} not as expected")

def main(argv=None):
    import numpy as np
    import time
    if isinstance(argv, str):
        import shlex
        argv = shlex.split(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-20s %(levelname)-8s  %(message)s")

    ## CCD-style
    root = '/tmp/testFits_ccd'
    os.makedirs(root, mode=0o755, exist_ok=True)
    t0 = time.time()
    testWrites(root=root)
    t1 = time.time()
    testWriteOutput(root=root)
    logging.info(f'ccd writes: {t1-t0:0.4f}')

    ## HX-style
    root = '/tmp/testFits_hx'
    os.makedirs(root, mode=0o755, exist_ok=True)
    t0 = time.time()
    testWrites(nread=100, root=root)
    t1 = time.time()
    testWriteOutput(nread=100, root=root)
    logging.info(f'hx writes: {t1-t0:0.4f}')

if __name__ == "__main__":
    main()
