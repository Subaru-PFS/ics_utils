from astropy import wcs

def pixelWcs():
    """Return an astropy.WCS which transforms pixel=(1,1) to pixel=(0,0) """

    w = wcs.WCS(naxis=2)
    w.wcs.crpix = [1.0, 1.0]
    w.wcs.cdelt = [1.0, 1.0]
    w.wcs.crval = [0.0, 0.0]
    w.wcs.ctype = ["LINEAR", "LINEAR"]

    return w

def pixelWcsCards():
    """Return a WCS which transforms pixel=(1,1) to pixel=(0,0)

    Returns a list of fitsio-compliant dictionaries. Built inline
    because Subaru does not accept some of the astropy.wcs cards.
    """

    cards = []
    cards.append(dict(name="COMMENT", value="#### Pixel-pixel WCS"))
    for ax_i in 1,2:
        cards.append(dict(name=f'CRPIX{ax_i}', value=1.0,
                          comment='Pixel coordinate of reference point'))
        cards.append(dict(name=f'CDELT{ax_i}', value=1.0,
                          comment='Coordinate increment at reference point'))
        cards.append(dict(name=f'CTYPE{ax_i}', value="LINEAR",
                          comment='Coordinate type code'))
        cards.append(dict(name=f'CRVAL{ax_i}', value=0.0,
                          comment='Coordinate value at reference point'))
        cards.append(dict(name=f'CUNIT{ax_i}', value='pixel',
                          comment='Coordinate units'))
    return cards
