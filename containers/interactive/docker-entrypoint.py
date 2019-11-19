#!/usr/bin/python

import os
import shlex
import sys

# os call to start gremlin-server on each instance
os.system("$JANUSGRAPH_HOME/bin/gremlin-server.sh")
