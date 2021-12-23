#!/usr/bin/env lua

local stack = require "stack"

local function doPrepare(line)
   local vars = stack.split(line, ' ')
   nargs = (#vars - 1)/2
   for i = 1, nargs do
      lamp = vars[2*i]
      time = vars[2*i + 1]
      if not stack.getOutlet(lamp) then error(string.format('unknown lamp : %s', lamp))
      end
      if not stack.toFloat(time) then error(string.format('cant convert %s on time : %s to float', lamp, time))
      end
   end
   stack.writeLamps(line)
   return 'OK'
end


return {doPrepare = doPrepare}

