#!/usr/local/bin/python
#==========================================================================
# Reads the list of stations and the available processed data for each station
# within a particular type (ie, WNAM_Filter_DetrendNeuTimeSeries_jpl, 
# WNAM_Clean_DetrendNeuTimeSeries_jpl, etc.).  For each data set,
# it creates a JSON  file (ie, WNAM_Filter_DetrendNeuTimeSeries_jpl_FILL.json) 
# that is consumed by DailyRdahmmSep.jsp.
#===========================================================================
import os, sys, string, re, json
from datetime import date, datetime, timedelta, time
from properties import properties

# Some useful global constants
today = datetime.today()
serverName = "gf9.ucs.indiana.edu"
updateTime = str(today.strftime("%Y-%m-%dT%H:%M:%S"))
beginDate = "1994-01-01"
endDate = str(today.strftime("%Y-%m-%d"))
centerLng = "-119.7713889"
centerLat = "36.7477778"
stateChangeNumTxtFile = "stateChangeNums.txt"
stateChangeNumJsInput = "stateChangeNums.txt.jsi"
allStationInputName = "all_stations.all.input"
filters = "Fill_Missing"
# stationCount="1532"

# Used to separate parts of the station name
SEPARATOR_CHARACTER="_"
NO_DATA_TIME_STAMP="22:22:22"
FINAL_PATH=properties('eval_path')

def setStationId(stationList, stationData):
    #Get the station name.
    stationName=stationList.split(SEPARATOR_CHARACTER)[2];
    stationData['id'] = stationName
    stationData['pro_dir'] = "daily_project_" + stationName + "_" + endDate
    stationData['AFile'] = "daily_project_" + stationName + ".A"
    stationData['BFile'] = "daily_project_" + stationName + ".B"
    stationData['InputFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input"
    stationData['RawInputFile'] = "daily_project_" + stationName + "_" + endDate + ".all.raw"
    stationData['SwfInputFile'] = "daily_project_" + stationName + "_" + endDate + ".plotswf.input"
    stationData['DygraphsInputFile'] = "daily_project_" + stationName + "_" + endDate + ".dygraphs.js"
    stationData['LFile'] = "daily_project_" + stationName + ".L"
    stationData['XPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.X.png"
    stationData['YPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.Y.png"
    stationData['ZPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.Z.png"
    stationData['XTinyPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.X_tiny.png"
    stationData['YTinyPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.Y_tiny.png"
    stationData['ZTinyPngFile'] = "daily_project_" + stationName + "_" + endDate + ".all.input.Z_tiny.png"
    stationData['PiFile'] = "daily_project_" + stationName + ".pi"
    stationData['QFile'] = "daily_project_" + stationName + "_" + endDate + ".all.Q"
    stationData['MaxValFile'] = "daily_project_" + stationName + ".maxval"
    stationData['MinValFile'] = "daily_project_" + stationName + ".minval"
    stationData['RangeFile'] = "daily_project_" + stationName + ".range"
    stationData['ModelFiles'] = "daily_project_" + stationName + ".zip"
    stationData['RefFile'] = "daily_project_" + stationName + ".input.ref"
    return

def setStationStartDate(stationDir, stationData):
    startFileName = stationDir + "daily_project_" + stationData['id'] + ".input.starttime"
    if (os.path.isfile(startFileName)):
        with open(startFileName,"r") as startFile:
            startDate = startFile.readline().rstrip()
        startFile.close()
    else:
	startDate = "1994-01-01"
    stationData['start_date'] = startDate
    return

def setStationRefLatLonHgt(stationDir, stationData):
    refFileName = stationDir + stationData['RefFile']
    refLat=""
    refLon=""
    refHgt=""
    if (os.path.isfile(refFileName)):
        with open(refFileName,"r") as refFile:
            refParts=refFile.readline().split(" ")
            refLat=refParts[0]
            refLon=refParts[1]
            refHgt=refParts[2].rstrip() # Have to chomp off the final \n
        refFile.close()
    else:
        refLat="1.0"
        refLon="2.0"
        refHgt="-1.0"

    stationData['lat'] = refLat      
    stationData['long'] = refLon      
    stationData['height'] = refHgt
    return

def setStatusChanges(stationDir, stationData):
    # Open the .all.Q and the .all.raw files.  We get the state from the first and
    # the data from the second. 
    # TODO: for now, we assume these files always exist
    qFileName = stationDir + stationData['QFile']
    rawFileName = stationDir + stationData['RawInputFile']
    
    # Bail out if the required files don't exist
    if((not os.path.isfile(qFileName)) or (not os.path.isfile(rawFileName))): 
        return 
    qFile = open(qFileName,"r")
    rawFile = open(rawFileName,"r")

    stateChanges = []
    changeCount = 0
    # Now step through the Q file looking for state changes
    # If we find a state change, get the date from the raw file
    # We will save these to the string stateChangeArray since we
    # need to record in latest-first order
    qline1 = qFile.readline()
    rline1 = rawFile.readline()        
    while True:
	eventData = {}
        qline2 = qFile.readline()
        rline2 = rawFile.readline()
        if not qline2: break
        
        # See if qline1 and qline2 are the same.  If so, extract the dates from rline1 and rline2
        # The line splits below are specific to the raw file line format.
        if (qline1.rstrip() != qline2.rstrip()):
            eventdate = rline2.split(" ")[1] 
            eventdate = eventdate.split("T")[0]
	    oldstate = qline1.rstrip()
 	    newstate = qline2.rstrip()
	    eventData['date'] = eventdate
	    eventData['from'] = oldstate
	    eventData['to'] = newstate 
            stateChanges.append(eventData) 
	    changeCount += 1
		
        # Make the previous "next" lines the "first" lines for the next comparison
        qline1=qline2
        rline1=rline2

    stationData['status_changes'] = stateChanges
    stationData['change_count'] = changeCount

    # Clean up
    qFile.close
    rawFile.close
    return

def setTimesNoData(stationDir, stationData):
    rawFileName = stationDir + stationData['RawInputFile']

    # Required file doesn't exist so bail out
    if(not os.path.isfile(rawFileName)): return
    rawFile = open(rawFileName, "r")
    
    noDataRanges = []
    noDataCount = 0
    noDataEvent = {}

    # We need to set a no-data range from beginDate (for the epoch, 1994-01-01) to the day before
    # our first data point for this station.  If the station has data before 1994-01-01, then 
    # ignore.
    firstDataDateParts=rawFile.readline().split(" ")[1].split("T")[0].split("-");

    beginEpoch=date(1994,1,1)

    #Convert this into a data object
    dayMinusOne=date(int(firstDataDateParts[0]),int(firstDataDateParts[1]),int(firstDataDateParts[2]))
    dayMinusOne-=timedelta(days=1)
    if(dayMinusOne > beginEpoch): 
        dayMinusOneString=dayMinusOne.isoformat()
        noDataEvent['to'] = dayMinusOneString
        noDataEvent['from'] = beginDate
        noDataCount += 1

    #Reset the "raw" file to the beginning
    rawFile.seek(0)

    # Step through the file to find the starting and ending dates with no data.
    # By convention, this occurs when the line has a timestamp T22:22:22.  Also, by
    # convention, we will record the latest to earliest dates with no data.

    while True:
        noDataEvent = {}
        nodata=False
        rline1=rawFile.readline()
        if not rline1: break

        # Get the date and timestamp, following format conventions
        fulleventdate1=rline1.split(" ")[1]
        eventdate1=fulleventdate1.split("T")[0]
        timestamp1=fulleventdate1.split("T")[1]

        # See if we have detected a no-data line
        if(timestamp1==NO_DATA_TIME_STAMP):
            nodata=True
            #Keep eventdate1 in case this is an isolated no-data line.
            eventdate_keep=eventdate1

            # We have a no-data line, so step ahead until the 
            # no-data line ends.
            while(nodata):
                rline2=rawFile.readline()
                if not rline2: break
                fulleventdate2=rline2.split(" ")[1]
                eventdate2=fulleventdate2.split("T")[0]
                timestamp2=fulleventdate2.split("T")[1]
                if(timestamp2!=NO_DATA_TIME_STAMP):
                    # Data exists for the second time stamp, so break out
                    # The last no-data line was the previous line
                    nodata=False
                    break
                else:
                    # No data for this line either, so keep this timestamp
                    # and start the while(nodata) loop again
                    eventdate_keep=eventdate2

            # We now know the range of no-data values, so insert this range, latest first
	    noDataEvent['to'] = eventdate_keep
	    noDataEvent['from'] = eventdate1
	    noDataRanges.append(noDataEvent)
	    noDataCount += 1
            
    # Finally, prepend the data-not-yet-available date range, from the last day of data
    # until today's date.
    today=date.today()
    formattedToday=today.isoformat() 
    
    #Reread the last event
    rawFile.seek(0)
    lastRawLine=rawFile.readlines()[-1]
    lastRawDate=lastRawLine.split(" ")[1].split("T")[0]
    lastDataDateParts=lastRawDate.split("-")  # This is the last date
    #Create a new date object out of the string we get from the file.
    lastDataDatePlus1=date(int(lastDataDateParts[0]),int(lastDataDateParts[1]),int(lastDataDateParts[2]))
    #Now increment this date one day.
    lastDataDatePlus1+=timedelta(days=1)    
    #Now convert to a string
    lastDataDataP1String=lastDataDatePlus1.isoformat()

    noDataEvent = {}
    noDataEvent['to'] = formattedToday
    noDataEvent['from'] = lastDataDataP1String
    noDataRanges.append(noDataEvent)
    noDataCount += 1
    
    stationData['time_nodata'] = noDataRanges
    stationData['nodata_count'] = noDataCount
    rawFile.close
    return

#--------------------------------------------------
# Now run the script for a given dataset. 
#--------------------------------------------------
# Set the properties
numargv = len(sys.argv)
if numargv == 1:
    sys.exit("usage: create_summary_jsons.py scripps_dataset_name")
elif numargv == 2:
    dataSet = sys.argv[1]
else:
    sys.exit("Invalid number of parameters!")

projectDir = FINAL_PATH + "/"+dataSet
if(os.path.isdir(projectDir)):
    # Open the JSON file that will contain the results
    outputPath = FINAL_PATH+"/"+dataSet + "_FILL.json"
    summaryData = {}
        
    summaryData['update_time'] = updateTime
    summaryData['data_source'] = dataSet
    summaryData['begin_date'] = beginDate
    summaryData['end_date'] = endDate
    summaryData['center_longitude'] = centerLng
    summaryData['center_latitude'] = centerLat
    summaryData['server_url'] = "http://" + serverName + "/daily_rdahmmexec/daily/" + dataSet
    summaryData['stateChangeNumTxtFile'] = stateChangeNumTxtFile
    summaryData['stateChangeNumJsInput'] = stateChangeNumJsInput
    summaryData['allStationInputName'] = allStationInputName
    summaryData['Filters'] = filters
    summaryData['video_url'] = ""

    stations = []
    stationCount = 0
# Extract the station name from the directory name. 
# Now loop over the station directories for each data set
    for stationList in os.listdir(projectDir):
        stationPath = projectDir + "/" + stationList + "/"
        if (os.path.isdir(stationPath)):
	    stationData = {}

            setStationId(stationList, stationData)
	    setStationStartDate(stationPath, stationData)
            setStationRefLatLonHgt(stationPath, stationData)
            setStatusChanges(stationPath, stationData)
            setTimesNoData(stationPath, stationData)

	    stations.append(stationData)
	    stationCount += 1 

    summaryData['stations'] = stations
    summaryData['station_count'] = stationCount
    #print summaryData
    with open(outputPath, 'w') as jsonfile:
	jsonfile.write(json.dumps(summaryData, sort_keys=True, indent=2))
    jsonfile.close
