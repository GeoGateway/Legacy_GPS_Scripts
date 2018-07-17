"""
	generate_jplsites.py
		-- generate a jplsites similar to unrsites.py
"""

from __future__ import print_function

import os
import sys
import urllib2


def get_jplsites():

	# Read table of positions and velocities
    response1 = urllib2.urlopen('http://sideshow.jpl.nasa.gov/post/tables/table2.html')
    lines = response1.read().decode('utf-8').splitlines()

    sites = []
    for i in range(0,len(lines)):
        test = lines[i].split()
        if (len(test) == 8):
            if (test[1] == 'POS'):
                sitecode = str(test[0])
                rlat = test[2]
                rlon = test[3]
                rheight = str(float(test[4]) / 1000.0) # mm -> meter
                sites.append([sitecode,rlat,rlon,rheight])

    return sites

def write_py(jplsites):
	""" output data as jplsites.py """
	with (open("jplsites.py","w")) as f:
		f.write("def jpl_sites():" + "\n")
		f.write("    datalist = [" + "\n")
		for entry in jplsites:
			site_str =",".join(["\'%s\'" % x for x in entry])
			site_str = "        [" + site_str + "],"
			f.write(site_str + "\n")
		f.write("        ]" + "\n")
		f.write("    return datalist" + "\n")

def main():
	jplsites = get_jplsites()
	print(len(jplsites))
	write_py(jplsites)

if __name__ == '__main__':
    main()
