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

def findCard(cards, cardName):
    """Return the index of a card in a card list

    Args
    ----
    cards : list of card dicts
       the list of cards to search through
    cardName : `str`
       the name of the cardto find.

    Returns:
    idx : the index of cardName in card, or -1
    """
    for c_i, c in enumerate(cards):
        if c['name'] == cardName:
            return c_i
    return -1

def popCard(cards, cardName):
    """Remove a given card from a card list and return it.

    Args
    ----
    cards : list of card dicts
       the list of cards to search through
    cardName : `str`
       the name of the cardto find.

    Returns:
    card : `dict` or None
      the card we removed, or None if not found.
    """
    idx = findCard(cards, cardName)
    if idx >= 0:
        return cards.pop(idx)

    return None

def replaceCard(cards, newCard):
    """Rewrite the content of a given card in a card list or append it.

    Args
    ----
    cards : list of card dicts
       the list of cards to search through
    newCard : `dict`
       the card we want

    Returns:
    replaced : `bool`
      True if we replaced the content in an existing card
    """
    idx = findCard(cards, newCard['name'])
    if idx >= 0:
        oldCard = cards[idx]
        oldCard['value'] = newCard['value']
        oldCard['comment'] = newCard['comment']
        return True
    else:
        cards.append(newCard)
        return False

def moveCard(fromCards, toCards, cardName):
    """Arrange for a card to be moved from one list to another, without changing order if possible.

    If the card already exists in toCards, replace the value and comment in place. Else append it. In
    either case, remove it from fromCards

    Any changes are made in place.

    Args
    ----
    fromCards : list of card dicts
       where to move the card from
    toCards : list of card dicts
       where to move the card to
    cardName : `str`
       the card whose value to move
    """

    cardToMove = popCard(fromCards, cardName)
    if cardToMove is None:
        return

    replaceCard(toCards, cardToMove)

def moveCards(fromCards, toCards):
    """Arrange for a list of card to be moved from one list to another, without changing order if possible.

    If any cards already exists in toCards, replace their values and comments in place. Else append them.

    All changes are made in place.

    Args
    ----
    fromCards : list of card dicts
       where to move the card from
    toCards : list of card dicts
       where to move the card to
    """

    for cardToMove in fromCards:
        replaceCard(toCards, cardToMove)
    fromCards.clear()
