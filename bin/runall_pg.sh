#!/bin/bash

# Send a given command to all the nodes present in an inventory table

TABLE=$1
shift
CMD=$*

psql -AXqtc "select instance_ip from $TABLE" | xargs -P32 -n 1 -I {} ssh root@{} $*
# Alternate command, run one at a time...
#psql -AXqtc "select instance_ip from $TABLE" | xargs -n 1 -I {} ssh root@{} $*
