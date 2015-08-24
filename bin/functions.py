# -*- coding: utf-8 -*-

import os
import random
import shlex
import subprocess
import sys
import time
from datetime import datetime

from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType

log = None

selected_availability_zone = None

def makedirs(testname, testdate):
    basedir = "/root/tests"
    os.makedirs("{0}/{1}/{2}/workloads".format(basedir, testname, testdate))
    os.makedirs("{0}/{1}/{2}/results".format(basedir, testname, testdate))
    os.makedirs("{0}/{1}/{2}/nodes/client".format(basedir, testname, testdate))
    os.makedirs("{0}/{1}/{2}/nodes/data".format(basedir, testname, testdate))

def record_test_parameters(testname, testdate, data_per_node, threads_per_client, comment = None):
    basedir = "/root/tests"
    f = open("{0}/{1}/{2}/comment".format(basedir, testname, testdate), "w")
    if comment:
        f.write('comment: ' + comment)
    f.write('\ndata per node: ' + str(data_per_node))
    f.write('\nthreads per client: ' + str(threads_per_client))
    f.write('\n')
    f.close()

def process_templates(dbtype, data_nodecount, client_nodecount, data_per_node, testname, testdate):
    basedir = "/root/tests"
    makedirs(testname, testdate)

    # Template the workload files
    recordsize = 10*10 # 100b records with 10 fields
    recordcount = data_per_node / recordsize * data_nodecount

    # If needed, adjust by database type to adjust for different storage format overhead
    if dbtype == 'cassandra':
        recordcount = int(recordcount * 1)
    elif dbtype == 'casscql':
        recordcount = int(recordcount * 1)
    elif dbtype == 'couchbase':
        recordcount = int(recordcount * 1)
    elif dbtype == 'hbase':
        recordcount = int(recordcount * 1)
    elif dbtype == 'mongodb':
        recordcount = int(recordcount * 1)

        # Each client will insert this many records
    insertcount = recordcount / client_nodecount

    # operationcount is based on an estimate of 10000 ops/sec so that each test runs around 15 minutes, but is capped at 1hr in the workload file
    operationcount = 9000000
    templatedata = {
        "recordcount": str(recordcount),
        "insertcount": str(insertcount),
        "operationcount": str(operationcount),
    }
    templatelist = os.listdir("{0}/workload_templates".format(basedir))
    templatelist.sort()

    for template in templatelist:
        tpl = open("{0}/workload_templates/{1}".format(basedir, template), "r")
        tpldata = tpl.read()
        tpl.close()
        tpl = open("{0}/{1}/{2}/workloads/{3}".format(basedir, testname, testdate, template), "w")
        tpl.write(tpldata.format(**templatedata))
        tpl.close()

    return templatelist

def openlog(testname, testdate):
    global log
    log = open("/root/tests/{0}/{1}/runtest.log".format(testname, testdate), "w")

def teelog(message):
    curr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if log:
        log.write(curr + " " + message + "\n")
    print message


def createtables(pgcursor, dbtype, testname, testdate):
    if dbtype == 'cassandra':
        pgcursor.execute("""CREATE TABLE {0}_{1}_data (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT false, keyspace text)""".format(testname, testdate))
        pgcursor.execute("""CREATE TRIGGER {0}_{1}_data_cassandra BEFORE INSERT ON {0}_{1}_data FOR EACH ROW EXECUTE PROCEDURE cassandra_new_node()""".format(testname, testdate))
    elif dbtype == 'casscql':
        pgcursor.execute("""CREATE TABLE {0}_{1}_data (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT false)""".format(testname, testdate))
    elif dbtype == 'couchbase':
        pgcursor.execute("""CREATE TABLE {0}_{1}_data (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT false)""".format(testname, testdate))
    elif dbtype == 'hbase':
        pgcursor.execute("""CREATE TABLE {0}_{1}_data (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT false)""".format(testname, testdate))
    elif dbtype == 'mongodb':
        pgcursor.execute("""CREATE TABLE {0}_{1}_data (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT false)""".format(testname, testdate))
    else:
        teelog( "Internal error, unknown dbtype " + dbtype )
        sys.exit(2)

    pgcursor.execute("""CREATE TRIGGER {0}_{1}_data_new AFTER INSERT ON {0}_{1}_data FOR EACH STATEMENT EXECUTE PROCEDURE new_node()""".format(testname, testdate))
    pgcursor.execute("""CREATE TABLE {0}_{1}_client (nodeid SERIAL PRIMARY KEY, instance_id text NOT NULL, instance_ip inet NOT NULL, started TIMESTAMPTZ NOT NULL DEFAULT now(), terminated TIMESTAMPTZ, ready BOOLEAN DEFAULT true)""".format(testname, testdate))
    pgcursor.execute("""CREATE TRIGGER {0}_{1}_client_new AFTER INSERT ON {0}_{1}_client FOR EACH STATEMENT EXECUTE PROCEDURE new_node()""".format(testname, testdate))

    pgcursor.connection.commit()


def spotrequest(ec2, nodetype, count, testname, testdate, threads, userdata = None):
    global selected_availability_zone

    maxprice = "0.40"
    if nodetype == 'client':
        instancetype = 'c3.xlarge'
    else:
        # For data nodes
        instancetype = 'c3.4xlarge'

    # Allow an explicit selection if needed...
    #selected_availability_zone = "us-east-1e"
    if not selected_availability_zone:
        selected_availability_zone = random.choice([
            'us-east-1a',
            #'us-east-1b',
            'us-east-1d',
            'us-east-1e',
        ])
    availability_zone = selected_availability_zone

    if userdata == None:
        userdata = """#!/bin/bash
echo {0} > /etc/node_testname
echo {1} > /etc/node_testdate
echo {2} > /etc/node_threads
echo {3} > /etc/node_role
#echo 10.136.71.116 > /etc/node_headnode
echo 400 > /etc/node_swap             # MB of swap created
echo 1 > /etc/node_mongo_uselocal     # Use local mongos shard server on each client
""".format(testname, testdate, threads, nodetype)

    # For some tests we may not need any nodes of this type
    if count == 0:
        return []

    # Default AMI
    ami = 'ami-XXXXXXXX' # Current versions

    # Specify ephemeral block devices...

    bdmap = BlockDeviceMapping()
    sdb = BlockDeviceType()
    sdb.ephemeral_name = 'ephemeral0'
    bdmap['/dev/sdb'] = sdb
    sdc = BlockDeviceType()
    sdc.ephemeral_name = 'ephemeral1'
    bdmap['/dev/sdc'] = sdc
    #sdd = BlockDeviceType()
    #sdd.ephemeral_name = 'ephemeral2'
    #bdmap['/dev/sdd'] = sdd
    #sde = BlockDeviceType()
    #sde.ephemeral_name = 'ephemeral3'
    #bdmap['/dev/sde'] = sde

    return ec2.request_spot_instances(maxprice, ami, count=count, launch_group=testdate, availability_zone_group=testdate, security_groups=['epstatic'], user_data=userdata, instance_type=instancetype, block_device_map=bdmap)

def demandrequest(ec2, nodetype, testname, testdate, threads, userdata = None):
    global selected_availability_zone

    if nodetype == 'client':
        instancetype = 'c3.xlarge'
    else:
        instancetype = 'i2.xlarge'

    # Allow an explicit selection if needed...
    #selected_availability_zone = 'us-east-1e'
    if not selected_availability_zone:
        selected_availability_zone = random.choice([
            'us-east-1a',
            #'us-east-1b',
            'us-east-1d',
            'us-east-1e',
        ])
    availability_zone = selected_availability_zone

    if userdata == None:
        userdata = """#!/bin/bash
echo {0} > /etc/node_testname
echo {1} > /etc/node_testdate
echo {2} > /etc/node_threads
echo {3} > /etc/node_role
echo 10.136.71.116 > /etc/node_headnode
echo 400 > /etc/node_swap             # MB of swap created
echo 1 > /etc/node_mongo_uselocal     # Use local mongos shard server on each client
""".format(testname, testdate, threads, nodetype)

    # Default AMI
    ami = 'ami-XXXXXXXX' # Current versions

    # Specify ephemeral block devices...

    bdmap = BlockDeviceMapping()
    sdb = BlockDeviceType()
    sdb.ephemeral_name = 'ephemeral0'
    bdmap['/dev/sdb'] = sdb
    #sdc = BlockDeviceType()
    #sdc.ephemeral_name = 'ephemeral1'
    #bdmap['/dev/sdc'] = sdc
    #sdd = BlockDeviceType()
    #sdd.ephemeral_name = 'ephemeral2'
    #bdmap['/dev/sdd'] = sdd
    #sde = BlockDeviceType()
    #sde.ephemeral_name = 'ephemeral3'
    #bdmap['/dev/sde'] = sde

    return ec2.run_instances(ami, placement=availability_zone, security_groups=['epstatic'], user_data=userdata, instance_type=instancetype, block_device_map=bdmap)

def createnodes(ec2, dbtype, data_nodecount, client_nodecount, threads, testname, testdate, userdata=None):
    # (client_nodecount) client nodes, and (data_nodecount) data nodes
    return spotrequest(ec2, 'client', client_nodecount, testname, testdate, threads, userdata) + spotrequest(ec2, dbtype, data_nodecount, testname, testdate, threads, userdata)

def createnodesnow(ec2, dbtype, data_nodecount, client_nodecount, threads, testname, testdate, userdata=None):
    # (client_nodecount) client nodes, and (data_nodecount) data nodes
    reservations = [demandrequest(ec2, 'client', testname, testdate, threads, userdata) for i in range(client_nodecount)] + [demandrequest(ec2, dbtype, testname, testdate, threads, userdata) for i in range(data_nodecount)]
    instances = []
    for reservation in reservations:
        for instance in reservation.instances:
            instances.append(instance.id)
    return instances

def awaitnodes(ec2, sir_list, timeout = 0):
    sleep_interval = 60 #seconds
    max_loops = timeout / sleep_interval
    nullcount = len(sir_list)
    sir_ids = []
    for sir in sir_list:
        sir_ids.append(sir.id)

    while nullcount > 0:
        nullcount = 0
        instance_ids = []
        time.sleep(sleep_interval)
        # Refresh the information we have...
        new_list = ec2.get_all_spot_instance_requests(request_ids=sir_ids)
        for sir in new_list:
            if sir.instance_id == None:
                nullcount += 1
            else:
                instance_ids.append(sir.instance_id)
        if nullcount > 0:
            if timeout != 0 and max_loops == 0:
                teelog( "Nodes still haven't appeared.  Exiting, please check it out manually." )
                sys.exit(2)
            teelog( "Still waiting on start-up of {0} nodes...".format(nullcount) )
            max_loops -= 1

    # Once every request has an instance_id, return that list
    return instance_ids

def tagnodes(ec2, instance_ids, name):
    reservations = ec2.get_all_instances(instance_ids=instance_ids)
    for reservation in reservations:
        instances = reservation.instances
        for instance in instances:
            instance.add_tag("Name", name)

def terminatenodes(ec2, instance_ids):
    reservations = ec2.get_all_instances(instance_ids=instance_ids)
    for reservation in reservations:
        instances = reservation.instances
        for instance in instances:
            instance.terminate()

def check_pgnodes(pgcursor, testname, testdate, data_nodecount, client_nodecount, instances, timeout=0):
    sleep_interval = 10 #seconds
    max_loops = timeout / sleep_interval
    clientsfound = 0
    serversfound = 0

    while clientsfound < client_nodecount or serversfound < data_nodecount:
        time.sleep(sleep_interval)
        pgcursor.execute("SELECT COUNT(*) FROM {0}_{1}_client".format(testname, testdate))
        row = pgcursor.fetchone()
        clientsfound = row[0]
        pgcursor.execute("SELECT COUNT(*) FROM {0}_{1}_data WHERE ready".format(testname, testdate))
        row = pgcursor.fetchone()
        serversfound = row[0]

        # Missing instance helper...
        instances_checked_in = []
        pgcursor.execute("SELECT instance_id FROM {0}_{1}_client UNION SELECT instance_id FROM {0}_{1}_data".format(testname, testdate))
        rows = pgcursor.fetchall()
        for row in rows:
            instances_checked_in.append(row[0])

        teelog( "I see {0}/{2} clients and {1}/{3} data servers reporting...".format(clientsfound, serversfound, client_nodecount, data_nodecount) )
        if clientsfound > 0 or serversfound > 0:
            teelog( "Missing Instances: {0}".format(",".join(set(instances).difference(set(instances_checked_in)))) )
        if clientsfound < client_nodecount or serversfound < data_nodecount:
            if timeout != 0 and max_loops == 0:
                teelog( "Nodes still haven't reported in.  Exiting, please check it out manually." )
                sys.exit(2)
            max_loops -= 1
        else:
            # Artificial sleep to give the nodes a little extra start-up time
            time.sleep(sleep_interval)

def get_pgnodes(pgcursor, testname, testdate, table, field, nodeid=None):
    where_clause = ""
    if nodeid != None:
        where_clause = " WHERE nodeid = '{0}'".format(nodeid)

    pgcursor.execute("SELECT {3} FROM {0}_{1}_{2}{4} ORDER BY nodeid".format(testname, testdate, table, field, where_clause))
    result = []
    pgresult = pgcursor.fetchall()
    for row in pgresult:
        if len(row) == 1:
            result.append(row[0])
        else:
            result.append(row)
    return result


def run_across_nodes(node_list, command):
    if type(node_list) is str:
        node_list = node_list.split(',')

    node_count = len(node_list)
    distribute_command_base = ['xargs', '-P', str(node_count), '-n', '1', '-I', '{}', 'ssh', 'root@{}']
    distribute_command = distribute_command_base + shlex.split(command)
    xarg_node_list = "\n".join(node_list)

    proc = subprocess.Popen(distribute_command, stdin=subprocess.PIPE)
    proc.communicate(xarg_node_list)
    if proc.returncode != 0:
        teelog( "Warning: Distribution of command across nodes resulted in return code {0}".format(proc.returncode)  )
    return proc.returncode

def update_client_datanode_list(client_nodes, data_nodes):
    datahosts = ",".join(data_nodes)
    run_across_nodes(client_nodes, 'echo "{0}" > /etc/node_datahosts'.format(datahosts))

def prepare_data_nodes(dbtype, data_nodes, master_node):
    # TODO: Calculate this from the outside caller
    insertcount = 500000000

    if dbtype == "cassandra":
        # Set up the schema on the master node
        cassandra_commands="""create keyspace usertable;
use usertable;
create column family data with compression_options = null;
"""
        cli_command=['ssh', 'root@{0}'.format(master_node), '/home/ubuntu/cassandra/current/bin/cassandra-cli']
        proc = subprocess.Popen(cli_command, stdin=subprocess.PIPE)
        proc.communicate(cassandra_commands)
        if proc.returncode != 0:
            teelog( "Error: Could not send cassandra commands to create keyspace, error {0}".format(proc.returncode) )

    if dbtype == "casscql":
        # Set up the schema on the master node
        time.sleep(10)
        cassandra_commands="""create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1 };
create table ycsb.usertable (
    y_id varchar primary key,
    field0 blob,
    field1 blob,
    field2 blob,
    field3 blob,
    field4 blob,
    field5 blob,
    field6 blob,
    field7 blob,
    field8 blob,
    field9 blob)
with compression = {'sstable_compression': ''};
"""
        cli_command=['ssh', 'root@{0}'.format(master_node), '/home/ubuntu/cassandra/current/bin/cqlsh']
        proc = subprocess.Popen(cli_command, stdin=subprocess.PIPE)
        proc.communicate(cassandra_commands)
        if proc.returncode != 0:
            teelog( "Error: Could not send cassandra commands to create keyspace, error {0}".format(proc.returncode) )

    if dbtype == "hbase":
        hbase_commands = "create 'usertable', {{NAME => 'data'}}, {{SPLITS => [{0}]}}".format(', '.join(["'user" + str(1000+x*(9999-1000)/200) + "'" for x in range(1,200)]))
        cli_command=['ssh', 'root@{0}'.format(master_node), '/home/ubuntu/hbase/current/bin/hbase shell']
        proc = subprocess.Popen(cli_command, stdin=subprocess.PIPE)
        proc.communicate(hbase_commands)
        if proc.returncode != 0:
            teelog( "Error: Could not send hbase commands to create table, error {0}".format(proc.returncode) )

    if dbtype == "mongodb":
        mongodb_commands = """sh.enableSharding("ycsb")
sh.shardCollection("ycsb.usertable", { "_id": 1})
use admin
"""
        # Try to pre-compute shard chunk assignments
        if len(data_nodes) > 1:
            mongodb_commands += "sh.stopBalancer()\n"
            mongodb_commands += "\n".join(["""db.runCommand({{split:"ycsb.usertable", middle: {{_id: "user{1}"}}}})\ndb.adminCommand({{moveChunk: "ycsb.usertable", find:{{_id: "user{1}"}}, to: "shard{0:04d}"}})""".format(x % len(data_nodes), 1000+x*(9999-1000)/200) for x in range(1,200)])
            mongodb_commands += "\nsh.startBalancer()\n"

        cli_command=['ssh', 'root@{0}'.format(master_node), '/home/ubuntu/mongodb/current/bin/mongo']
        proc = subprocess.Popen(cli_command, stdin=subprocess.PIPE)
        proc.communicate(mongodb_commands)
        if proc.returncode != 0:
            teelog( "Error: Could not send mongodb commands enable database sharding, error {0}".format(proc.returncode) )

def test_run(dbtype, client_nodes, workload, command="run"):
    command_path = "/root/bin/client/"
        verbose = "-v"
    if command == "run":
        verbose = ""
    run_across_nodes(client_nodes, "{0}{1} {2} {3}".format(command_path, command, workload, verbose))

def pull_from_node(node_address, node_directory, remote_files):
    pull_command = ["rsync", "-a", "{0}:{1}".format(node_address, remote_files[0])] \
            + [":" + remote_file for remote_file in remote_files[1:]] \
            + ['{0}/'.format(node_directory)]
    proc = subprocess.Popen(pull_command)
    proc.communicate()

def pull_results(pgcursor, dbtype, testname, testdate):
    basedir = "/root/tests"

    client_nodes = get_pgnodes(pgcursor, testname, testdate, "client", "nodeid,instance_ip")
    data_nodes = get_pgnodes(pgcursor, testname, testdate, "data", "nodeid,instance_ip")

    for node in client_nodes:
        (nodeid,address) = node
        run_across_nodes([address], "vnstat --dumpdb > vnstat.db")
        run_across_nodes([address], "vnstat -h > vnstat.h")
        run_across_nodes([address], "/usr/lib/sysstat/sa2 -C -A")

        node_directory = "{0}/{1}/{2}/nodes/client/{3}".format(basedir, testname, testdate, str(nodeid))
        os.makedirs(node_directory)
        pull_files = ["vnstat.db", "vnstat.h", "/mnt/", "/var/log/sysstat"]
        if dbtype == "mongodb":
            pull_files.append("/var/log/mongodb/")

        pull_from_node(address, node_directory, pull_files)

    for node in data_nodes:
        (nodeid,address) = node
        run_across_nodes([address], "vnstat --dumpdb > vnstat.db")
        run_across_nodes([address], "vnstat -h > vnstat.h")
        run_across_nodes([address], "sar -C -A > sar.out")
        run_across_nodes([address], "dmesg > dmesg.log")
        if dbtype == "mongodb":
            run_across_nodes([address], "/root/bin/mongodb/export_mongo") # Small script to export sharding stats
            run_across_nodes([address], "tar -cJf /mnt/mongod.log.tar.xz /var/log/mongodb")
        elif dbtype == "couchbase":
            run_across_nodes([address], "tar -cJf /mnt/couchbase.log.tar.xz /opt/couchbase/var/lib/couchbase/logs") # Small script to export sharding stats
        elif dbtype == "hbase":
            run_across_nodes([address], "tar -cJf /mnt/hbase.log.tar.xz /home/ubuntu/h*/current/logs") # Small script to export sharding stats

        node_directory = "{0}/{1}/{2}/nodes/data/{3}".format(basedir, testname, testdate, str(nodeid))
        os.makedirs(node_directory)
        pull_files = ["vnstat.db", "vnstat.h", "sar.out", "dmesg.log", "df.start*", "df.end", "/var/log/sysstat"]
        if dbtype == "cassandra" or dbtype == "casscql":
            pull_files.append("/var/log/cassandra/")
        #elif dbtype == "couchbase":
        #    pull_files.append("/home/ubuntu/couchbase/current/var/lib/couchbase/logs/")
        elif dbtype == "hbase":
            pull_files.append("/mnt/hbase.log.tar.xz")
        elif dbtype == "mongodb":
            pull_files.append("mongo_stats.txt")

        pull_from_node(address, node_directory, pull_files)
