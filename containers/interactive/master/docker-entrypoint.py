#!/usr/bin/python

import os
import shlex
import sys
import subprocess
import time

# os call to start gremlin-server on each instance
subprocess.Popen(['memcached', '-m', '5000', '-u', 'nobody'])

# copy janusgraph-cassandra cluster settings from worker1
subprocess.call(['scp', 'worker1:/sgp/janusgraph/conf/gremlin-server/janusgraph-cassandra-es-server.properties', '/sgp/scripts/conf/'])

# copy remote-objects.yaml file from worker1 for client driver settings
subprocess.call(['scp', 'worker1:/sgp/janusgraph/conf/remote-objects.yaml', '/sgp/scripts/conf/'])

# easy hack to keep entry point script running forever
while True:
	time.sleep(60)
