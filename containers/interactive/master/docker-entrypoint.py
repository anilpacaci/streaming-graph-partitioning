#!/usr/bin/python

import os
import shlex
import sys

# os call to start gremlin-server on each instance
os.system("memcached -m 5000")

os.system("scp worker1:/sgp/janusgraph/conf/gremlin-server/janusgraph-cassandra-es-server.properties /sgp/scripts/conf/")
