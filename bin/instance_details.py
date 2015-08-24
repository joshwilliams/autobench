#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from boto.ec2.connection import EC2Connection

def instancedetails(ec2, instance_ids):
	reservations = ec2.get_all_instances(instance_ids=instance_ids)
	for reservation in reservations:
		instances = reservation.instances
		for instance in instances:
			print "Instance {0}".format(instance.id)
			print "State: {0}".format(instance.state)
			print "Private IP: {0}".format(instance.private_ip_address)
			print

if len(sys.argv) < 2:
	print "Usage: {0} instance-id [instance-id ...]".format(sys.argv[0])
	sys.exit(1)

ec2 = EC2Connection()

instancedetails(ec2, sys.argv[1:])
