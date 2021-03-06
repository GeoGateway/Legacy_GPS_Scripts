#!/usr/local/bin/python
#==========================================================================
# Ingest a given UNR dataset into the corresponding databases. 
# To be invoked by the overall unr_ingest.py using subprocess, 
# destination data directory and temporary working directory are defined in
# properties.
#
# input: UNR dataset name (IGS08 or FID)
# output: corresponding overall sqlite db file with all data ingested;
#   as well as duplicate-filled sqlite db file for individual stations 
#
# usage:
#   unr_ingest_single.py igs08|fid
#
# output:
#   /path/to/rdahmm/unr_data.sqlite
#   /path/to/rdahmm/stationID.sqlite
#
# reference: 
#   JPL web: https://sideshow.jpl.nasa.gov/post/series.html
#   documents: https://sideshow.jpl.nasa.gov/post/tables/GPS_Time_Series.pdf
#
# Time Series and Residual Format
#   Column 1: Decimal_YR  
#   Columns 2-4: East(m) North(m) Vert(m) 
#   Columns 5-7: E_sig(m) N_sig(m) V_sig(m) 
#   Columns 8-10: E_N_cor, E_V_cor, N_V_cor
#   Column 11: Time in Seconds past J2000
#   Columns 12-17: Time in YEAR MM DD HR MN S
#===========================================================================
import os, sys, string 
import sqlite3 as db
import urllib2
from datetime import date
from datetime import timedelta
from datetime import datetime

from properties import properties
from jplsites import jpl_sites

from time import strptime
from time import mktime


station_list = jpl_sites()
#sample_url: ftp://sideshow.jpl.nasa.gov/pub/usrs/mbh/point/019B.series
url_prefix = "ftp://sideshow.jpl.nasa.gov/pub/usrs/mbh/point/"
url_suffix = ".series"
# jpl currently has only one dataset
dataset = "point"

data_path = properties('data_path')
temp_path = properties('temp_path')
model_path = properties('model_path')

datadir = data_path + "JPL_" + dataset.upper() + "/"
dbfile = datadir + "JPL_" + dataset.upper() + ".sqlite"
workdir = temp_path + "JPL_" + dataset.upper() + "/"
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

for entry in station_list:
    (stationID, lat, long, height) = entry

    url = url_prefix + stationID + url_suffix
    # skip this part first
    # try:
    #   last_mod = urllib2.urlopen(url).info().getdate('last-modified')
    #   print(last_mod)
    #   last_mod_ts = datetime(last_mod[:7]) 
    #   today = datetime.today()
    #   if (today - last_mod_ts) > timedelta(days=180):
    #     print stationID + " inactive for over 180 days, skipping."
    #     continue
    # except urllib2.URLError:
    #     print stationID + " not available for download."
    #     continue
    wgetcmd = "wget -nv -P " + workdir + " " + url 
    os.system(wgetcmd)

    sql = "INSERT INTO ReferencePositions (StationID, Latitude, Longitude, Height) "
    sql += " VALUES ('%s', '%s', '%s', '%s')" % (stationID, lat, long, height)
    cur.execute(sql)

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

    with open(workdir + stationID + url_suffix, 'r') as f: 
        data = f.readlines()
    last_line = ""
    for line in data[1:]:
        record = string.split(line)
        #   Column 1: Decimal_YR  
        #   Columns 2-4: East(m) North(m) Vert(m) 
        #   Columns 5-7: E_sig(m) N_sig(m) V_sig(m) 
        #   Columns 8-10: E_N_cor, E_V_cor, N_V_cor
        #   Column 11: Time in Seconds past J2000
        #   Columns 12-17: Time in YEAR MM DD HR MN S

        # convert meter to mm
        east = float(record[1])*1000.0
        north = float(record[2])*1000.0
        up = float(record[3])*1000.0
        esig = float(record[4])*1000.0
        nsig = float(record[5])*1000.0
        usig = float(record[6])*1000.0
        #2001  4 20
        sts = record[11] + "/" + record[12] + "/" + record[13]
        timestamp = date.fromtimestamp(mktime(strptime(sts, "%Y/%m/%d")))
        sql = "INSERT INTO GPSTimeSeries (StationID, North, East, Up, Nsig, Esig, Usig, Timestamp) "
        sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (stationID, north, east, up, nsig, esig, usig, timestamp)
        try: 
            cur.execute(sql)
        except db.IntegrityError as error: 
            print "Source data error: ", sql
            print "sqlite3.IntegrityError: ", error, "\n"
            continue
                
        if last_line == "":
            last_line = line
        else:
            last_record = string.split(last_line)
            lsts = last_record[1]
            least = float(last_record[8])*1000.0
            lnorth = float(last_record[10])*1000.0
            lup = float(last_record[12])*1000.0
            lesig = float(last_record[14])*1000.0
            lnsig = float(last_record[15])*1000.0
            lusig = float(last_record[16])*1000.0
            last_timestamp = date.fromtimestamp(mktime(strptime(lsts, "%y%b%d")))
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
