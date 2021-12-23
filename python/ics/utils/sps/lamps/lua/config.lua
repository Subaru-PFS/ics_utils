#!/usr/bin/env lua

-- network config
local address = "127.0.0.1"
local port = 9000

-- outlets config
local lnames={"halogen", "neon", "hgar", "argon", "krypton"}
local loutlets={ 1, 2, 3, 4, 5}


return {address = address, port = port, lnames = lnames, loutlets = loutlets}