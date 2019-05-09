#!/usr/bin/python

import sys
import re
from os import listdir

def get_cumulative(filename):
        finalCumulative = ""

        with open(filename) as openfileobject:
            for line in openfileobject:
                if 'Cumulative' in line:
                        finalCumulative = line

        cumulativeCommunication = int(float(re.findall("(\S*)GB", finalCumulative)[-1]) * 1000)
        print filename, ",", cumulativeCommunication

def main():
    directory = sys.argv[1]

    for result_file in listdir(directory):
        get_cumulative(result_file)


if __name__ == "__main__":
    main()
