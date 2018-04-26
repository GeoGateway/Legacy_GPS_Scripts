#!/usr/local/bin/python

#================================================
# Generate unavco station list with corresponding reference positions
#
# usage:
#   python make_unavco_meta.py station_list_file
#
# output:
#   station_list_file.ref  
#============================================

import sys, os, subprocess 

numargv = len(sys.argv)
unavco_client_path = "./unavcoClients"

if numargv == 1:
    sys.exit("usage: make_unavco_meta.py station_list_file")
elif numargv == 2:
    station_file = sys.argv[1]
else:
    sys.exit("Invalid number of parameters!")

with open(station_file, 'r') as f:
    lines = f.read()
f.close()

lines = lines.split("\n")[1:-1]
for entry in lines:
    stationID = entry.split(",")[0]
    cmd = "java -jar " + unavco_client_path + "/unavcoMetadata.jar -4charEqu " + stationID.upper()
    metadata = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE).communicate()[0].split("\n")
    refs = metadata[1].split(",")
    if len(refs) < 5:
	continue
    (elev, lat, lon) = refs[2:5]
    print str([stationID, lon, lat, elev])
