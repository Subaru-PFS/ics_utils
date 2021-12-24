# just a quick installation guide

* copy lua code from ics_config/lua to the digitalLoggers controller
     `scp -r lua/digitalLoggers/* admin@pdu2-dcb:/storage/current`

* add the following line to /etc/rc.local to start script at startup
    `/storage/current/launch >/tmp/server.log 2>&1 &`

* notes that the configuration is stored in config.lua (host, port, outlets...)
