#!/usr/bin/python

import os
import shlex
import sys
import subprocess
import time

# os call to start gremlin-server on each instance
subprocess.Popen(['memcached', '-m', '16384', '-u', 'nobody'])

# copy janusgraph-cassandra cluster settings from worker1
subprocess.call(['scp', 'worker1:/sgp/janusgraph/conf/gremlin-server/janusgraph-cassandra-es-server.properties', '/sgp/scripts/conf/'])

# easy hack to keep entry point script running forever
while True:
	time.sleep(60)
