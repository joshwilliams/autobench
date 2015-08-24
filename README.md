# autobench
A set of scripts used to run automated benchmarks on AWS.

This repository contains the driver scripts developed to benchmark NoSQL type databases in a more automated way on AWS.  The repo primarily contains the scripts that run on the control node, sometimes referred to as the head node.

The repository also contains helper scripts for the worker node, which contains the software to run either the YCSB client or a database instance.  These aren't used directly on the control node, nd instead are expected to be placed into the worker node's AMI.

For an idea about how all this fits together, take a look at the "Big Picture" section below.

These scripts were originally built up as one-off helper scripts, and unfortunately as a result code maintainability and care to avoid repetition took a back seat.

## The control node
The repo's contents originally lived in root's home (as a few hardcoded paths may indicate) on a single constantly running instance.  It doesn't necessarily have to run on AWS, just so long as the worker nodes can talk back to its Postgres instance and via SSH.

As designed, the control node will run a Postgres instance: see db/postgres\_functions.sql.  This database is really there to give the worker nodes somewhere to talk back to register their presence and receive a node ID assignment.

With a bit of creativity, that could be replaced with something else, for example a directory of files.  Alternatively allowing all connections in unauthenticated (by setting trust in pg\_hba.conf) would reduce the hurdles of keeping Postgres in place.

### The scripts
* bin/runtest.py is the main script.  This is called with primarily two arguments: the type of database and the number of data nodes to start.

* bin/functions.py is pulled in as a Python module, and contains most of the real code.

## The worker node
As mentioned above, the worker node scripts are expected to be embedded into an AMI, as the control node (or the instance's start up process itself) will call them when needed.

The bin directory is expected to be /root/bin/, while etc contains a few configuration files to help tune the start up process and have sysstat gather system metrics at a very frequent 10-second inverval.  The databases are expected to be in /home/ubuntu/ but could be moved around anywhere, so long as the configure\_\* scripts can find them.

Make sure SSH keys are set up on the AMI instance, as well as the head node, as there's quite a bit of communication between the instances.  The worker-node/bin/cleanami script can be used to shut down services and clean up a template instance to prep it for building an image, without needing to have it rebooted each time.

## The Big Picture
1. Run bin/runtest.py *database-type* *instance-count*
2. Postgres database tables are set up to allow the instances a place to call back to.
3. The workload templates are processed, and written out to define the parameters depending on *instance-count*.
4. A request is made to AWS to start up *instance-count* data nodes of the requested type, plus a calculated number of client nodes, in a random availability zone.
  * The same AMI is used for both, but different configuration is passed via the user-data portion of the request.
  * Ideally spot instances are used, and if so the script will wait for the spot instances to be fulfilled before continuing.  If the desired instance type isn't supported by spot instances, a combination can be used so they'll at least work for the client instances.
5. Once the instances are provisioned, they're tagged with the database type to help differentiate them from other concurrently running tests, and help identify them through the AWS console.
6. Then it waits.  The database tables are checked periodically until the number of instances seen matches the number of instances requested.
  * Note that it doesn't necessarily expect the same instances it originally requested.  Occasionally some AWS instances fail to start, and this allows replacement instances to be requested (by giving the same user-data blob.)
7. Meanwhile, each worker node starts up:
  1. cloud-init runs /root/bin/find\_ephemeral to build a RAID-0 across all available ephemeral volumes, and mount it on /mnt.
  2. /root/bin/phone\_home is run via cron to report back that the instance is available, and then kicks off the /root/bin/configure\_[node-type] script.  Those scripts do the actual work of setting up the database shard, given the node ID, IP addresses of other nodes that have checked in, etc.  Client nodes pull down the processed workload files from the control node.
  3. Similarly each database type has its own db\_ready script, which checks the database state and only continues when all background processing has completed.
  4. When each shard node has started up completely, it reports back again to update its ready status.
8. When all the instances have reported in, the control node proceeds.  Each of the client nodes is simultaneously contacted in parallel and issued the command to run the YCSB load process.
9. When all client instances complete the load, all the data nodes are contacted to run their db\_ready script, ensuring that all background optimization processes have completed after the load process.
10. For each workload file:
    * The control node similarly connects to each client node, and issues the command to run the YCSB workload.  Performance data is held on each client instance for now.  After each workload, the db\_ready script is run again.
11. After all workloads have completed, the head node cycles through each instance, and rsync's down all the data.  This primarily includes the YCSB performance data, but also includes system metrics to help with any debugging or additional performance evaluation, as well as database system logs.
12. All worker nodes for the test are terminated.
