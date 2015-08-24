#!/usr/bin/env python
# -*- coding: utf-8 -*-

# rsync down results from nodes on demand
# Super simple helper script whittled down from the test driver

import psycopg2
import sys

from functions import *

if len(sys.argv) < 3:
	print "Usage: {0} testname testdate".format(sys.argv[0])
	sys.exit(1)

# XXX: Figure out via testname
dbtype = "hbase"

# Test parameters derived from data node count, and other things:
testname = sys.argv[1]
testdate = sys.argv[2]

pg = psycopg2.connect("dbname=root user=root")
pgcur = pg.cursor()

pull_results(pgcur, dbtype, testname, testdate)

