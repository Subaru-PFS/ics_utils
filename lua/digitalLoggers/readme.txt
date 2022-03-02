# just a quick installation guide

* access to the webpage (default pfs login and pwd).
  if not configured : `admin,1234`

* you need ssh access in the first place, which can be activated from the webpage.

* copy lua code from ics_config/lua to the digitalLoggers controller
     `scp -r lua/digitalLoggers/* pfs@pdu2-dcb:/storage/current`

* add the following line to /etc/rc.local to start script at startup
    `/storage/current/launch >/tmp/server.log 2>&1 &`

* notes that the configuration is stored in config.lua (host, port, outlets...)
