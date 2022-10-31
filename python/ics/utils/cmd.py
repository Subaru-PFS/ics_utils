import re


def pullOutException(trace):
    """ Try to pull out the full exception trace. """
    start = re.search("(?<=command failed: )", trace).end()
    end = re.search("(?<= in )(.*)(?= at )", trace).end()
    return trace[start:end]


def interpretFailure(cmdVar):
    """ return text which should explain that cmdVar failure. """
    cmdKeys = cmdVarToKeys(cmdVar)

    if 'Timeout' in cmdKeys:
        return f"TimeoutError('reached in {cmdVar.timeLim} secs')"
    elif 'NoTarget' in cmdKeys:
        return 'actor is not connected'
    elif 'text' in cmdKeys:
        trace = cmdKeys['text'].values[0]
        try:
            failure = pullOutException(trace)
        except:
            failure = trace
        return failure
    else:
        return 'unknown reason'


def formatLastReply(cmdVar):
    """Typical formatting."""
    return cmdVar.replyList[-1].keywords.canonical(delimiter=';')


def cmdVarToKeys(cmdVar):
    """ Convert cmdVar keyword as dictionary."""
    return dict(sum([[(k.name, k) for k in reply.keywords] for reply in cmdVar.replyList], []))


def parse(cmdStr, **kwargs):
    """ parse keyword argument to the cmdStr in a mhs compliant way. """
    args = []
    for k, v in kwargs.items():
        if v is None or v is False:
            continue

        if isinstance(v, list):
            v = ','.join([str(e) for e in v])

        args.append(k if v is True else f'{k}={v}')

    return ' '.join([cmdStr.strip()] + args)


def findCmdKey(cmdStr, cmdKey):
    """Find cmdKey in cmdStr and return it."""
    # don't even bother to go further.
    cmdKey = f'{cmdKey}='

    if re.search(cmdKey, cmdStr) is None:
        return None

    idlm = re.search(cmdKey, cmdStr).span(0)[-1]
    sub = cmdStr[idlm:]
    sub = sub if sub.find(' ') == -1 else sub[:sub.find(' ')]
    pattern = f' {cmdKey}{sub[0]}(.*?){sub[0]}' if sub[0] in ['"', "'"] else f' {cmdKey}{sub}'
    m = re.search(pattern, cmdStr)
    return m.group()


def stripCmdKey(cmdStr, cmdKey):
    """Strip given text cmdKey from cmdStr."""
    cmdKeyStr = findCmdKey(cmdStr, cmdKey)

    if not cmdKeyStr:
        return cmdStr

    # just remove that cmdKey.
    return cmdStr.replace(cmdKeyStr, '').strip()


def findCmdKeyValue(cmdStr, cmdKey):
    """Just return value contained in cmdKey from cmdStr."""
    cmdKeyStr = findCmdKey(cmdStr, cmdKey)

    if not cmdKeyStr:
        return None

    __, value = cmdKeyStr.split(f'{cmdKey}=')
    return value
