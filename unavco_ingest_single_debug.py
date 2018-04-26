#!/usr/local/bin/python
#==========================================================================
# Ingest a given UNAVCO dataset into the corresponding databases. 
# To be invoked by the overall unavco_ingest.py using subprocess, 
# destination data directory and temporary working directory are defined in
# properties.
#
# input: unavco dataset name (pbo or nucleus)
# output: corresponding overall sqlite db file with all data ingested;
#   as well as duplicate-filled sqlite db file for individual stations 
#
# usage:
#   unavco_ingest_single.py pbo|nucleus
#
# output:
#   /path/to/rdahmm/unavco_data.sqlite
#   /path/to/rdahmm/stationID.sqlite
#===========================================================================
import os, sys, string 
import sqlite3 as db
import urllib2
from datetime import date
from datetime import timedelta
from datetime import datetime

from properties import properties
from unavcosites import pbo_sites, nucleus_sites

numargv = len(sys.argv)
if numargv == 1:
    sys.exit("usage: unavco_ingest_single.py pbo|nucleus")
elif numargv == 2:
    dataset = sys.argv[1]
else:
    sys.exit("Invalid number of parameters!")

if dataset == 'pbo':
    station_list = pbo_sites()
elif dataset == 'nucleus':
    station_list = nucleus_sites()
else:
    sys.exit("Unrecognized dataset: " + dataset)

#rdahmm_path = "/home/yuma/RDAHMM/Data/"
data_path = properties('data_path')

datadir = data_path + "UNAVCO_" + dataset.upper() + "/"
dbfile = datadir + "UNAVCO_" + dataset.upper() + ".sqlite"
#print datadir, dbfile

if not os.path.exists(datadir):
    cmd = "mkdir -p " + datadir
    os.system(cmd) 

#if the same db file exists, drop it
if os.path.isfile(dbfile):
    print "deleting old database"
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

today = date.today().isoformat() + "T00:00:00"
url_prefix = "http://www.unavco.org/ws/gps/data/position/"
url_suffix = "/beta?refframe=igs08&starttime=1980-01-06T00:00:00&endtime=" + today

for entry in station_list:
    (stationID, long, lat, height) = entry
    sql = "INSERT INTO ReferencePositions (StationID, Latitude, Longitude, Height) "
    sql += " VALUES ('%s', '%s', '%s', '%s')" % (stationID, lat, long, height)
    cur.execute(sql)

    station_dbfile = datadir + stationID + ".sqlite"
    if os.path.isfile(station_dbfile):
        print "deleting old station database"
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

    url = url_prefix + stationID + url_suffix
    print url;
    records = urllib2.urlopen(url).read()
    records = records.split("\n")[1:-1]
    last_item = ""
    for item in records:
        [ctimestamp, north, east, up, nsig, esig, usig] = [x.strip() for x in item.split(',')]
        timestamp = datetime.strptime(ctimestamp, "%Y-%m-%dT%H:%M:%S")
        sql = "INSERT INTO GPSTimeSeries (StationID, North, East, Up, Nsig, Esig, Usig, Timestamp) "
        sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (stationID, north, east, up, nsig, esig, usig, timestamp.isoformat()[:10])
        cur.execute(sql)
                
        if last_item == "":
            last_item = item
        else:
            [ltimestamp, lnorth, least, lup, lnsig, lesig, lusig] = [x.strip() for x in last_item.split(',')]
            last_timestamp = datetime.strptime(ltimestamp, "%Y-%m-%dT%H:%M:%S")
            # if missing days from last to current, fill with last
            for i in range(1, (timestamp - last_timestamp).days):
                ts = last_timestamp + timedelta(days=i)
                interploated = 1
                station_sql = "INSERT INTO StationGPSTimeSeries (North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated) "
                station_sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (lnorth, least, lup, lnsig, lesig, lusig, ts.isoformat()[:10], interploated)
                station_cur.execute(station_sql)
            last_item = item

        station_sql = "INSERT INTO StationGPSTimeSeries (North, East, Up, Nsig, Esig, Usig, Timestamp) "
        station_sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (north, east, up, nsig, esig, usig, timestamp.isoformat()[:10])
        station_cur.execute(station_sql)

    station_conn.commit()
    conn.commit()
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
