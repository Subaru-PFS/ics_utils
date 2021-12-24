#!/usr/bin/env lua
local ipaddress = require("ipaddress")


-- network config
local address = ipaddress.getAutoIp(0)
local port = 9000

-- override auto-ip if necessary but it should work.
if false then
    address = "127.0.0.1"
end

-- outlets config
local lnames={"halogen", "neon", "hgar", "argon", "krypton"}
local loutlets={ 1, 2, 3, 4, 5}


return {address = address, port = port, lnames = lnames, loutlets = loutlets}