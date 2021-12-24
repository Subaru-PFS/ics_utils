#!/usr/bin/env lua

local socket = require("socket")

local function localIp()
    -- find out your local ip by creating a udp socket, yeek but working.
    local s = socket.udp()
    s:setpeername("google.com",80)
    local ip, _ = s:getsockname()
    return ip
end

local function getAutoIp(nAttempt)
    -- loop until an ip has been assigned.
    local ip = localIp()
    local notDefined = "0.0.0.0"
    local maxAttempt = 10
    local waitBetweenAttempt = 3

    if ip == notDefined and nAttempt<maxAttempt then
       --print(string.format('nAttempt#%d : %s', nAttempt, ip))
       socket.sleep(waitBetweenAttempt)
       return getAutoIp(nAttempt+1)
    end

    -- no longer looping check the result
    --if ip==notDefined then
    --   print(string.format('ip : %s  failed to get address after %d attempts', ip, nAttempt))
    --else
    --   print(string.format('nAttempt#%d : %s', nAttempt, ip))
    -- end

    return ip
end


return { getAutoIp = getAutoIp }
