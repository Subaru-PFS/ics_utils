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
    # actors that do not respect that convention.
    outLaws = ['gen2']

    try:
        if actorName in outLaws:
            raise ValueError

        [instanceNumber] = re.findall('[0-9]+', actorName)
        if '_' in actorName:
            [productName, instanceName] = actorName.split('_')
        else:
            [productName, __] = actorName.split(instanceNumber)
            instanceName = actorName

    except ValueError:
        # if not any number, productName and instanceName are the same.
        productName = instanceName = actorName

    return productName, instanceName