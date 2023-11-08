#!/usr/bin/env lua
local ipaddress = require("ipaddress")


-- network config
local address = ipaddress.getAutoIp(0)
local port = 9000

-- override auto-ip if necessary but it should work.
if false then
    address = "127.0.0.1"
end

-- outlets config for dcb1
local lnames={"halogen", "neon", "hgar", "argon", "krypton", "allFiberLamp", "filterwheel"}
local loutlets={ 1, 2, 3, 4, 5, 6, 7}


return {address = address, port = port, lnames = lnames, loutlets = loutlets}