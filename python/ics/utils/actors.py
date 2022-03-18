import re


def findProductAndInstance(actorName):
    """Find product name and instance name from actor name.

    Parameters
    ----------
    actorName : `str`
       Actor name.
    Returns
    -------
    productName, instanceName : `str`, str`
    """

    try:
        [instanceNumber] = re.findall('[0-9]+', actorName)
    except ValueError:
        return actorName, None

    try:
        [productName, instanceName] = actorName.split('_')
    except ValueError:
        [productName, __] = actorName.split(instanceNumber)
        instanceName = actorName

    return productName, instanceName
