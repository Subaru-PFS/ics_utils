#!/usr/bin/env lua
local config = require "config" -- don't add `.lua` to your `require` call
local uom = require "uom"
local stack = require "stack"
local socket = require("socket")

local function getState()
    local states = {}
    for i = 1, #(config.lnames) do
        bool = uom.relay.outlets[config.loutlets[i]].state
        table.insert(states, string.format('%s=%s', config.lnames[i], stack.boolToState(bool)))
    end
    return table.concat(states, ',')
end

local function switch(lamp, state)
    local bool = stack.stateToBool(state)
    local nOutlet = stack.getOutlet(lamp)
    if not nOutlet then
        error(string.format('unknown lamp : %s', lamp))
    else
        uom.relay.outlets[nOutlet].state = bool
    end
    bool = uom.relay.outlets[nOutlet].state
    return string.format('%s=%s', lamp, stack.boolToState(bool))
end

local function switchAllLampsOff()
    -- only the existing lamps
    local lamps = {"halogen", "neon", "hgar", "argon", "krypton", "xenon"}

    nlamp = #config.lnames
    for i = 1, nlamp do
        -- check if that outlet is actually a lamp or not.
        if stack.hasValue(lamps, config.lnames[i]) then
            uom.relay.outlets[config.loutlets[i]].state = false
        end
    end
end

local function fireLamps(client)
    local offset = 0.00  -- 200 ms turning-off time
    line = stack.readLamps()
    local vars = stack.split(line, ' ')

    nargs = #vars / 2
    nlamp = #config.lnames

    -- switch all lamps off at the beginning of the sequence.
    switchAllLampsOff()

    local times = {}
    local lamps = {}
    local olets = {}
    local doAbort = false

    -- gather parameters and initialize
    k = 1
    for i = 1, nargs do
        lamp = vars[2 * i]
        time = vars[2 * i + 1] * 1.000

        for j = 1, nlamp + 1 do
            if (lamp == config.lnames[j]) then
                lamps[k] = lamp
                olets[k] = config.loutlets[j]
                times[k] = time - offset

                k = k + 1
                break
            end
        end
        -- run off end if no match
        if (j == nlamp + 1) then
            client:send('unknown lamp')
            --client:send(string.format("Lamp %s is not a known lamp name, ignoring", lamp))
        end
    end
    nch = k - 1

    longest = 0
    ilong = 0

    for i = 1, nch do
        client:send(string.format("%-08s %01d %6.2f\n", lamps[i], olets[i], times[i]))
        if (times[i] > longest) then
            longest = times[i]
            ilong = i
        end
    end

    client:send(string.format("%01d channels active, longest %s %.2f seconds\n", nch, lamps[ilong], longest))
    client:send(string.format('%stcpover\n', getState()))

    start = {}
    stop = {}
    state = {}

    -- calculate stop times and turn on lamps
    for i = 1, nch do
        start[i] = socket.gettime()
        stop[i] = start[i] + times[i]
        uom.relay.outlets[olets[i]].state = true
        state[i] = true
        client:send(string.format('%s=ontcpover\n', lamps[i]))

    end
    -- loop and turn off lamps at appropriate times
    client:settimeout(3)
    nextEvent = stack.getNextEvent(state, stop)
    while (true)
    do
        time = socket.gettime()
        -- client:send(time)
        if nextEvent - time > 5 then
            local line, err = client:receive()
            if line and string.find(line, "abort") then
                doAbort = true
                client:send('tcpover\n')
            end

        end
        for i = 1, nch do
            if (((state[i] == true) and (time > stop[i])) or doAbort) then
                uom.relay.outlets[olets[i]].state = false
                state[i] = false
                client:send(string.format('%s=offtcpover\n', lamps[i]))
                nextEvent = stack.getNextEvent(state, stop)
            end
        end

        if (state[ilong] == false) then
            break
        end
    end

    -- switch all lamps off at the end.
    switchAllLampsOff()

    client:settimeout(10)
    return getState()

end

return { getState = getState, switch = switch, fireLamps = fireLamps }




