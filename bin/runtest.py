#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import os
import sys
import time
from datetime import datetime

import psycopg2
from boto.ec2.connection import EC2Connection
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType

from functions import *

if len(sys.argv) < 3 or not sys.argv[2].isdigit() or not sys.argv[1] in ['cassandra','casscql','couchbase','hbase','mongodb']:
	print "Usage: {0} dbtype nodecount\n  ... where dbtype is cassandra, casscql, couchbase, hbase, or mongodb".format(sys.argv[0])
	sys.exit(1)

# Drop the SSH_AUTH_SOCK variable
del os.environ['SSH_AUTH_SOCK']

dbtype = sys.argv[1]
data_nodecount = int(sys.argv[2])
# Approximate a 3:1 ratio of data nodes to client nodes
client_nodecount = int(math.ceil(data_nodecount/3.0))
# ... unless we want an equal number of data and client nodes
#client_nodecount = data_nodecount

test_notes = None
if len(sys.argv) == 4:
	test_notes = sys.argv[3]

# Test parameters derived from data node count, and other things:
testname = "{0}_{1}".format(dbtype, data_nodecount)
testdate = datetime.now().strftime("%Y%m%d_%H%M")
data_per_node = 50000000000 # Goal: 500M records per node
threads_per_client = 256 * data_nodecount / client_nodecount # 256 per data node, spread across the client nodes

# Service connections...
pg = psycopg2.connect("dbname=root user=root")
pgcur = pg.cursor()

ec2 = EC2Connection()

# Perform the test...
workloads = process_templates(dbtype, data_nodecount, client_nodecount, data_per_node, testname, testdate)
record_test_parameters(testname, testdate, data_per_node, threads_per_client, test_notes)
openlog(testname, testdate)

teelog( "This is test {0} at {1}...".format( testname, testdate ) )
teelog( "Creating local database tables..." )
createtables(pgcur, dbtype, testname, testdate)

teelog( "Requesting node allocation from AWS..." )
# If doing pure spot instances...
#sir_list = createnodes(ec2, dbtype, data_nodecount, client_nodecount, threads_per_client, testname, testdate)
#instances = awaitnodes(ec2, sir_list)
# Or if doing pure on-demand instances...
#instances = createnodesnow(ec2, dbtype, data_nodecount, client_nodecount, threads_per_client, testname, testdate)
# Now, attempting to use spot instances for client nodes only
instances = createnodesnow(ec2, dbtype, data_nodecount, 0, threads_per_client, testname, testdate)
sir_list = createnodes(ec2, dbtype, 0, client_nodecount, threads_per_client, testname, testdate)
instances += awaitnodes(ec2, sir_list)

time.sleep(10)
tagnodes(ec2, instances, testname)

teelog( "Checking that all nodes have reported in..." )
check_pgnodes(pgcur, testname, testdate, data_nodecount, client_nodecount, instances)
client_nodes = get_pgnodes(pgcur, testname, testdate, "client", "instance_ip")
data_nodes = get_pgnodes(pgcur, testname, testdate, "data", "instance_ip")
master_node = get_pgnodes(pgcur, testname, testdate, "data", "instance_ip", 1)[0]
time.sleep(10)

teelog( "Telling client nodes about the data nodes..." )
update_client_datanode_list(client_nodes, data_nodes)

teelog( "Setting up data nodes..." )
prepare_data_nodes(dbtype, data_nodes, master_node)

teelog( "Data nodes are {0}\nClient nodes are {1}".format(" ".join(data_nodes), " ".join(client_nodes)) )
teelog( "Verify cluster looks good and then press any key to continue the test.." )
raw_input('')

# XXX: Temporarily reduce threads for MongoDB and CouchBase load
if testname.startswith('mongodb') or testname.startswith('couchbase'):# or testname.startswith('cass'):
    teelog( "Reducing client threads" )
    run_across_nodes(client_nodes, "echo 40 > /etc/node_threads")

teelog( "Initiating data load..." )
run_across_nodes(data_nodes+client_nodes, "/usr/lib/sysstat/sadc -C \\'Initiating data load...\\' -")

# Assume the first workload file found is the load parameters
test_run(dbtype, client_nodes, workloads[0], "load")
run_across_nodes(data_nodes, "df /mnt > df.start") # Snapshot the disk utilization after the load process
run_across_nodes(data_nodes+client_nodes, "/usr/lib/sysstat/sadc -C \\'Load finished.\\' -")

teelog( "Load finished.  Checking database." )
run_across_nodes(data_nodes, "/root/bin/db_ready load") # Wait until any background processing is done

# XXX: Restore threads after MongoDB and CouchBase load
if testname.startswith('mongodb') or testname.startswith('couchbase'):# or testname.startswith('cass'):
    teelog( "Restoring client threads" )
    run_across_nodes(client_nodes, "echo {0} > /etc/node_threads".format(threads_per_client))

for workload in workloads[1:]:
    run_across_nodes(data_nodes, "df /mnt > df.start_{}".format(workload))
    teelog( "Running workload {0}...".format(workload) )
    run_across_nodes(data_nodes+client_nodes, "/usr/lib/sysstat/sadc -C \\'Running workload {0}\\' -".format(workload))
    test_run(dbtype, client_nodes, workload)
    run_across_nodes(data_nodes+client_nodes, "/usr/lib/sysstat/sadc -C \\'Workload {0} finished.\\' -".format(workload))
    teelog( "Workload {0} finished.  Checking database.".format(workload) )
    run_across_nodes(data_nodes, "/root/bin/db_ready {0}".format(workload)) # In case there's additional background processes

run_across_nodes(data_nodes, "df /mnt > df.end")

teelog( "Retrieving results from all nodes to local storage..." )
pull_results(pgcur, dbtype, testname, testdate)

teelog( "Test run complete, terminating nodes..." )
terminatenodes(ec2, instances)

