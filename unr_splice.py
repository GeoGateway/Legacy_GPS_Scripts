#!/usr/local/bin/python
#==========================================================================
# Generate UNR spliced dataset based on IGS08 and FID ingestion results.  
# destination data directory and temporary working directory are defined in
# properties.
#
# input: N/A 
# output: corresponding overall sqlite db file with all data ingested;
#   as well as duplicate-filled sqlite db file for individual stations 
#
# usage:
#   unr_splice.py 
#
# output:
#   /path/to/rdahmm/unr_data.sqlite
#   /path/to/rdahmm/stationID.sqlite
#===========================================================================
import os, sys, string 
import sqlite3 as db

from properties import properties

#numargv = len(sys.argv)
#if numargv == 1:
#    sys.exit("usage: unr_ingest_single.py igs08|fid")
#elif numargv == 2:
#    dataset = sys.argv[1]
#else:
#    sys.exit("Invalid number of parameters!")

model_path = properties('model_path')
igs_model_path = model_path + "/UNR_IGS08/"
fid_model_path = model_path + "/UNR_FID/"

data_path = properties('data_path')
igs_datadir = data_path + "/UNR_IGS08/"
fid_datadir = data_path + "/UNR_FID/"
datadir = data_path + "/UNR_SPLICE/"

igs_dbfile = igs_datadir + "UNR_IGS08.sqlite"
fid_dbfile = fid_datadir + "UNR_FID.sqlite"
dbfile = datadir + "UNR_SPLICE.sqlite"
#print datadir, dbfile

if not os.path.exists(datadir):
    cmd = "mkdir -p " + datadir
    os.system(cmd) 

#if the same db file exists, drop it
if os.path.isfile(dbfile):
#    print "deleting old database " + dbfile
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

igs_conn = db.connect(igs_dbfile)
igs_cur = igs_conn.cursor()

fid_conn = db.connect(fid_dbfile)
fid_cur = fid_conn.cursor()


for station in os.listdir(fid_model_path):
    stationID = station[-4:]
     
    #if os.path.exists(datadir + stationID + ".sqlite"):
    #	continue	

    fid_station_dbfile = fid_datadir + stationID + ".sqlite"
    igs_station_dbfile = igs_datadir + stationID + ".sqlite"

    if not os.path.exists(fid_station_dbfile):
	continue
    if not os.path.exists(igs_station_dbfile):
	continue

    igs_sql = "SELECT StationID, Latitude, Longitude, Height FROM ReferencePositions WHERE StationID = '%s'" % stationID
    igs_row = igs_cur.execute(igs_sql).fetchone()
    if igs_row == None:
	continue
    sql = "INSERT INTO ReferencePositions (StationID, Latitude, Longitude, Height) "
    sql += " VALUES ('%s', '%s', '%s', '%s')" % igs_row
    cur.execute(sql)

    igs_sql = "SELECT MAX(Timestamp) FROM GPSTimeSeries WHERE StationID = '%s'" % stationID
    igs_timestamp = igs_cur.execute(igs_sql).fetchone()[0]

    igs_sql = "SELECT StationID, North, East, Up, Nsig, Esig, Usig, Timestamp FROM GPSTimeSeries WHERE StationID = '%s'" % stationID
    igs_rows = igs_cur.execute(igs_sql).fetchall()
    for row in igs_rows:
        sql = "INSERT INTO GPSTimeSeries(StationID, North, East, Up, Nsig, Esig, Usig, Timestamp) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % row
        cur.execute(sql)

    fid_sql = "SELECT StationID, North, East, Up, Nsig, Esig, Usig, Timestamp FROM GPSTimeSeries WHERE StationID = '%s' AND Timestamp > '%s'" % (stationID, igs_timestamp)
    fid_rows = fid_cur.execute(fid_sql).fetchall()
    for row in fid_rows:
        sql = "INSERT INTO GPSTimeSeries(StationID, North, East, Up, Nsig, Esig, Usig, Timestamp) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % row
        cur.execute(sql)
    
    station_dbfile = datadir + stationID + ".sqlite"
    if os.path.isfile(station_dbfile):
#        print "deleting old station database " + station_dbfile
        os.remove(station_dbfile)
	#continue
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

    
    igs_station_conn = db.connect(igs_station_dbfile)
    igs_station_cur = igs_station_conn.cursor()

    fid_station_conn = db.connect(fid_station_dbfile)
    fid_station_cur = fid_station_conn.cursor()

    igs_station_sql = "SELECT North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated FROM StationGPSTimeSeries" 
    igs_rows = igs_station_cur.execute(igs_station_sql).fetchall()
    for row in igs_rows:
        station_sql = "INSERT INTO StationGPSTimeSeries(North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % row
        station_cur.execute(station_sql)

    fid_station_sql = "SELECT North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated FROM StationGPSTimeSeries WHERE Timestamp > '%s'" % igs_timestamp
    fid_rows = fid_station_cur.execute(fid_station_sql).fetchall()
    for row in fid_rows:
        station_sql = "INSERT INTO StationGPSTimeSeries(North, East, Up, Nsig, Esig, Usig, Timestamp, Interploated) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % row
        station_cur.execute(station_sql)

    station_conn.commit()
    conn.commit()
    station_cur.close()
    station_conn.close()
    
    igs_station_cur.close()
    igs_station_conn.close()
    fid_station_cur.close()
    fid_station_conn.close()

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

igs_cur.close()
igs_conn.close()

fid_cur.close()
fid_conn.close()
