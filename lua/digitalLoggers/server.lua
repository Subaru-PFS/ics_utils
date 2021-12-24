#!/usr/bin/env lua
local config = require "config" -- don't add `.lua` to your `require` call
local prepare = require "prepare"
local stack = require "stack"
local go = require "go"

function commandHandling(newLine, client)
    local cmdArgs = stack.split(newLine, ' ')
    if cmdArgs[1] == 'prepare' then
        return formatResponse(pcall(prepare.doPrepare, newLine))
    elseif cmdArgs[1] == 'getState' then
        return formatResponse(pcall(go.getState))
    elseif cmdArgs[1] == 'getOutletsConfig' then
        return formatResponse(pcall(stack.getOutletsConfig))
    elseif cmdArgs[1] == 'switch' then
        return formatResponse(pcall(go.switch, cmdArgs[2], cmdArgs[3]))
    elseif cmdArgs[1] == 'go' then
        return formatResponse(pcall(go.fireLamps, client))
    else
        return formatResponse(false, 'unknown command')
    end
end

function formatResponse(status, err)
    if not status then
        strStat = 'FAILED'
    else
        strStat = 'OK'
    end
    return string.format('%s;;%s', strStat, err)
end


-- load namespace
local socket = require("socket")
-- create a TCP socket and bind it to the local host, at any port
local server = assert(socket.bind(config.address, config.port))
-- find out which port the OS chose for us
local address, port = server:getsockname()
-- print a message informing what's up
print(string.format("Starting tcp server %s %d", address, port))

local eol = 'tcpover\n'
-- loop forever waiting for clients
while 1 do
    -- wait for a connection from any client
    local client = server:accept()
    -- make sure we don't block waiting for this client's line
    client:settimeout(10)
    -- receive the line
    local line, err = client:receive()
    -- if there was no error, send it back to the client
    if not err
    then
        local ret = commandHandling(line, client)
        client:send(string.format('%s%s', ret, eol))

    end

    -- done with client, close the object
    client:close()
end
