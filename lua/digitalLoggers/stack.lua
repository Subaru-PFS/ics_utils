#!/usr/bin/env lua
local config = require "config" -- don't add `.lua` to your `require` call

local function getOutlet(key)
    local names = config.lnames
    for i = 1, #names do
        if names[i] == key then
            return config.loutlets[i]
        end
    end
    return false
end

local function getOutletsConfig()
    local parseConf = {}
    for i = 1, #(config.lnames) do
        table.insert(parseConf, string.format('outlet0%d=%s', config.loutlets[i], config.lnames[i]))
    end
    return table.concat(parseConf, ',')
end

local function toFloat(value)
    flt = tonumber(value)
    if flt == nil then
        return false
    end
    return flt
end

local function split(str, pat)
    local t = {}  -- NOTE: use {n = 0} in Lua-5.0
    local fpat = "(.-)" .. pat
    local last_end = 1
    local s, e, cap = str:find(fpat, 1)

    while s do
        if s ~= 1 or cap ~= "" then
            table.insert(t, cap)
        end
        last_end = e + 1
        s, e, cap = str:find(fpat, last_end)
    end
    if last_end <= #str then
        cap = str:sub(last_end)
        table.insert(t, cap)
    end
    return t
end

local function hasValue (tab, val)
    for index, value in ipairs(tab) do
        if value == val then
            return true
        end
    end

    return false
end

local function writeLamps(line)
    file = io.open("/tmp/lampargs", "w")
    io.output(file)
    io.write(line)
    io.close(file)
end

local function readLamps(line)
    file = io.open("/tmp/lampargs", "r")
    io.input(file)
    local line = io.read()
    io.close(file)
    return line
end

local function stateToBool(state)
    if state == 'on' then
        return true
    elseif state == 'off' then
        return false
    else
        error(string.format('unknown state : %s', state))
    end
end

local function boolToState(bool)
    if bool == true then
        return 'on'
    elseif bool == false then
        return 'off'
    else
        error(string.format('unknown bool : %s', bool))
    end
end

local function getNextEvent(state, stop)
    local minTime = 0
    for i = 1, #state do
        if state[i] then
            if minTime==0 then
                minTime = stop[i]
            else
                minTime = math.min(minTime, stop[i])
            end

        end
    end
    return minTime
end

return { getOutlet = getOutlet, getOutletsConfig = getOutletsConfig, toFloat = toFloat, split = split, hasValue = hasValue, writeLamps = writeLamps, readLamps = readLamps, stateToBool = stateToBool, boolToState = boolToState, getNextEvent = getNextEvent }