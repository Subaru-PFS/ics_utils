def astropyCardsToFitsio(astropyCards):
    """Convert a list of astropy-compliant cards to a list of fitsio-compliant dictionaries.
    """

    cards = []
    for c in astropyCards:
        cards.append(dict(name=c[0], value=c[1], comment=c[2]))

    return cards

def astropyHeaderToFitsio(hdr):
    """Convert an astropy fits Header to a list of fitsio-compliant dictionaries.
    """

    return astropyCardsToFitsio(hdr.cards)
