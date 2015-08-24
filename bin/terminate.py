#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Manually terminate a bunch of instances

import sys

from boto.ec2.connection import EC2Connection

def terminatenodes(ec2, instance_ids):
	reservations = ec2.get_all_instances(instance_ids=instance_ids)
	for reservation in reservations:
		instances = reservation.instances
		for instance in instances:
			instance.terminate()

if len(sys.argv) < 2:
	print "Usage: {0} instance-id [instance-id ...]".format(sys.argv[0])
	sys.exit(1)

ec2 = EC2Connection()

terminatenodes(ec2, sys.argv[1:])
