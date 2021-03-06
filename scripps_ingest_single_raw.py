#!/usr/local/bin/python
#==========================================================================
# Ingest a given scripps dataset into the corresponding databases. 
# To be invoked by the overall scripps_ingest.py using subprocess, 
# destination data directory and temporary working directory are defined in
# properties.
#
# input: path to the original scripps tar file; 
# output: corresponding overall sqlite db file with all data ingested;
#   as well as duplicate-filled sqlite db file for individual stations 
#
# usage:
#   scripps_ingest_single_raw.py /path/to/download/scripps_data.tar
#
# output:
#   /path/to/rdahmm/scripps_data.sqlite
#   /path/to/rdahmm/stationID.sqlite
# 
# This is a clone of scripps_ingest_single.py with minor differences to handle 
# format discrepencies of rawNeuTimeSeries.MEASURES_Combination tar files:
#	- use gunzip (vs. unzip) to unpack data
#	- reference position lat/lon parsing differently
#	- reference position height missing  
#	- data units in m instead of mm
#	- days of the year is counted starting from 1
#
#
# We will only handle stations in the WNAMsites file, and skip others. 
#===========================================================================
import os, sys, string, re
import sqlite3 as db
from datetime import date
from datetime import timedelta
from properties import properties

from wnamsites import wnamsites

numargv = len(sys.argv)
if numargv == 1:
    sys.exit("usage: scripps_ingest_single_raw.py /path/to/scripps_data.tar")
elif numargv == 2:
    [scripps_path, tarfile] = os.path.split(sys.argv[1])
    scripps_path += "/"
else:
    sys.exit("Invalid number of parameters!")

station_list = wnamsites()

#rdahmm_path = "/home/yuma/RDAHMM/Data/"
#temp_path = "/home/yuma/RDAHMM/TEMP/"
data_path = properties('data_path')
temp_path = properties('temp_path')

#datadir = data_path + tarfile[:tarfile.rfind("_")] + "/"
datadir = data_path + tarfile[:-13] + "/"
#dbfile = datadir + tarfile[:-4] + ".sqlite"
# get rid of timestamp from db file name
dbfile = datadir + tarfile[:-13] + ".sqlite"
workdir = temp_path + tarfile[:-13] + "/"
#workdir = temp_path + tarfile[:tarfile.rfind("_")] + "/"
#print datadir, dbfile

if not os.path.exists(datadir):
    cmd = "mkdir -p " + datadir
    os.system(cmd) 
if not os.path.exists(workdir):
    cmd = "mkdir -p " + workdir
    os.system(cmd) 

#if the same db file exists, drop it
if os.path.isfile(dbfile):
    print "deleting old database " + dbfile
    os.remove(dbfile)

# creating/connecting the database 
conn = db.connect(dbfile)
# creating a Cursor
cur = conn.cursor()
# creating tables
sql ="""CREATE TABLE GPSTimeSeries (
      StationID CHAR(4), 
      North Num,
      East Num,
      Up  Num,
      Nsig Num,
      Esig Num, 
      Usig Num,
      Timestamp TEXT,
      UNIQUE (StationID, Timestamp))"""
cur.execute(sql)
sql ="""CREATE TABLE ReferencePositions (
      StationID CHAR(4), 
      Latitude Num,
      Longitude Num, 
      Height Num, 
      UNIQUE (StationID))"""
cur.execute(sql)
conn.commit()

# clear working directory
cmd = "rm -f " + workdir + "*"
os.system(cmd)

print "Processing ", tarfile, "..." 
# unpack data 
cmd = "tar xvf " + scripps_path + tarfile + " -C " + workdir
os.system(cmd)
dirlist = os.listdir(workdir)
dirlist.sort()
for datafile in dirlist:
    if datafile[-2:] == ".Z":
        if os.stat(workdir+datafile).st_size == 0: # When .Z file is empty
            continue
        os.chdir(workdir)
        cmd = "gunzip " + workdir + datafile 
        os.system(cmd)
        datafile = datafile[:-2]
        stationID = datafile[:4]

	if not stationID in station_list:
	    continue

        station_dbfile = datadir + stationID + ".sqlite"
        if os.path.isfile(station_dbfile):
            print "deleting old station database " + station_dbfile
            os.remove(station_dbfile)
        station_conn = db.connect(station_dbfile)
        station_cur = station_conn.cursor()
        station_sql ="""CREATE TABLE StationGPSTimeSeries (
                      North Num,
                      East Num,
                      Up  Num,
                      Nsig Num,
                      Esig Num, 
                      Usig Num,
                      Timestamp TEXT,
                      Interploated INT Default 0,
                      UNIQUE(Timestamp))"""
        station_cur.execute(station_sql)
        station_conn.commit()

    with open(workdir + datafile, 'r') as f:
        data = f.readlines()
        last_line = ""
        for line in data:
	    if "#" in line:
	        if "Latitude" in line:
		    lat = float(line.split(":")[-1])
	        if "Longitude" in line:
		    long = float(line.split(":")[-1])
	        if "Note" in line: # last line of comments
	            height = -9999 # height not available in the data source
                    sql = "INSERT INTO ReferencePositions (StationID, Latitude, Longitude, Height) "
                    sql += " VALUES ('%s', '%s', '%s', '%s')" % (stationID, lat, long, height)
                    cur.execute(sql)
            #if not "#" in line:
            else:
                record = string.split(line)
		if len(record) < 9: # When missing white spaces between columns
		    tmpstr = ' '.join(record[3:])
                    record[3:9] = re.findall(r"[+-]?\d+\.\d\d", tmpstr)
                [year, days] = map(int, record[1:3])
	        # days in source is counted starting from 1
                timestamp = date.fromordinal(date(year,1,1).toordinal()+days-1)
                [north, east, up, nsig, esig, usig] = [float(x)*1000 for x in record[3:9]]
                sql = "INSERT INTO GPSTimeSeries (StationID, North, East, Up, Nsig, Esig, Usig, Timestamp) "
                sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (stationID, north, east, up, nsig, esig, usig, timestamp)
                cur.execute(sql)
                
                if last_line == "":
                    last_line = line
                else:
                    last_record = string.split(last_line)
		    if len(last_record) < 9:
		        tmpstr = ' '.join(last_record[3:])
                        last_record[3:9] = re.findall(r"[+-]?\d+\.\d\d", tmpstr)
                    [year, days] = map(int, last_record[1:3])
	            # days in source is counted starting from 1
                    last_timestamp = date.fromordinal(date(year,1,1).toordinal()+days-1)
                    [lnorth, least, lup, lnsig, lesig, lusig] = [float(x)*1000 for x in last_record[3:9]]
                    # if missing days from last to current, fill with last
                    for i in range(1, (timestamp - last_timestamp).days):
                        ts = last_timestamp + timedelta(days=i)
                        interploated = 1
                        station_sql = "INSERT INTO StationGPSTimeSeries (North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated) "
                        station_sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (lnorth, least, lup, lnsig, lesig, lusig, ts, interploated)
                        station_cur.execute(station_sql)
                    last_line = line

                station_sql = "INSERT INTO StationGPSTimeSeries (North, East, Up, Nsig, Esig, Usig, Timestamp) "
                station_sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (north, east, up, nsig, esig, usig, timestamp)
                station_cur.execute(station_sql)

    station_conn.commit()
    conn.commit()
    f.closed
    station_cur.close()
    station_conn.close()
    #insert duplicates for missing days in the station database

# create index 
sql = "CREATE INDEX idx_StationID ON GPSTimeSeries(StationID)"
cur.execute(sql)
sql = "CREATE INDEX idx_Timestamp ON GPSTimeSeries(Timestamp)"
cur.execute(sql)
sql = "CREATE INDEX idx_RefStationID ON ReferencePositions(StationID)"
cur.execute(sql)

conn.commit()

cur.close()
conn.close()

# clear working directory
#cmd = "rm -f " + workdir + "*"
#os.system(cmd)
