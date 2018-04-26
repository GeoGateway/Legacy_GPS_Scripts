#!/usr/local/bin/python
#==========================================================================
# Creates the combined "all_stations.all.input" file for all the stations
# associated with a particular data set type. That is, we will create
# this file of combined inputs for WNAM_Filter_DetrendNeuTimeSeries_jpl,
# WNAM_Clean_DetrendNeuTimeSeries_jpl, etc.
#
# We choose 2004-01-01 as the starting date for this file.  The format is
# a series of columns as follows: time, stations1-East, station1-North, station1-Up,
# station2-East, station2-North, station2-Up,..., stationN-East, stationN-North, stationN-Up.
#==========================================================================
import os, sys, string, re
from datetime import date, datetime, timedelta, time
from properties import properties
import linecache


# Useful constants
START_EPOCH="1994-01-01"
BASE_OUTPUT_DIR="./"
SPACE=" "

def getStationName(stationDir):
    stationName=stationDir.split("_")[2]
    return stationName

def getStationAllRawFile(stationsFullPath):
    rawFileName=""
    valueSet=False
    for file in os.listdir(stationsFullPath):
        if(file.endswith(".all.raw")):
            rawFileName=file
            valueSet=True
            break
    if valueSet:
        return stationsFullPath+"/"+rawFileName
    else:
        raise Exception("Station directory"+stationsFullPath+" has no .all.raw file")

# Append ${station}-x, ${station}-y, and ${station-z} as column headings
def appendColumnHeadings(stationName,outputTmpList):
    newline=outputTmpList[0]+SPACE+stationName+"-x"+SPACE+stationName+"-y"+SPACE+stationName+"-z"
    outputTmpList[0]=newline
    return 

# This is a utility function that converts an isoformatted date string to a date object.
def convertIsoStringToDate(isostringdate):
    # Create a fullfledged date object from the string
    splitdatestamp=isostringdate.split("-")
    theDate=date(int(splitdatestamp[0]),int(splitdatestamp[1]),int(splitdatestamp[2]))
    return theDate

# Inspect the raw data line and extract the date as a date object.
def extractRawDataDate(rawline):
    # The date and time will be the second entry in the line.
    fullTimestamp=rawline.split()[1]
    # Split off the date from the time of day.
    datestamp=fullTimestamp.split("T")[0]
    theDate=convertIsoStringToDate(datestamp)
    duplicate=False
    if(fullTimestamp.split("T")[1]=="22:22:22"): duplicate=True
    return (theDate,duplicate)

# Write the time column with header "time". 
def writeTimeColumn(outputTmpList):
    outputTmpList.append("time")
    iterday=convertIsoStringToDate(START_EPOCH)
    while iterday<=date.today():
        outputTmpList.append(iterday.isoformat())
        iterday+=timedelta(days=1)
    return 

# Write NaN for stations that don't have data at the given time stamp
def writeNaNColumn(outputTmpList,index):    
    nanList=["NaN","NaN","NaN"]
    # Need to get the original line
    appendLineColumns(nanList,outputTmpList,index)
    return

# Write station data for given line
def writeStationColumns(line,outputTmpList,lineindex):
    splitline=line.split(" ")
    stationdata=[splitline[2],splitline[3],splitline[4]]
    appendLineColumns(stationdata,outputTmpList,lineindex)
    return 

# Write the station columns to the composite output file
def appendLineColumns(moreCols, outputTmpList, index):
    newline=outputTmpList[index]
    for column in moreCols:
        newline+=SPACE+column
    outputTmpList[index]=newline
    return

# A station may have data before the epoch starts, so skip these
def handleStationHasDataBeforeEpoch(epochDate,stationAllRaw,outputTmpList):
    with open(stationAllRaw,"r") as allRawFile:
        while True:
            line=allRawFile.readline()
            # The station data ended before the epoch began, so return out of the
            # whole function. We don't expect this to happen.
            if not line:
                print "WARNING: Station " +stationName+ " has no data after "+START_EPOCH
                return
            
            # Check the date.  If we are up to the epoch start date, break out.  Otherwise,
            # continue the while and read the next line.
            dateStampDate=extractRawDataDate(line)[0]
            if(dateStampDate>=epochDate):
                break
        allRawFile.close()
    return

def handlePriorToStationStartDate(epochDate,stationStartDate,outputTmpList,lineindex):
    iterday=epochDate
    while True:
        if(stationStartDate<=iterday):
            # We have iterated to the first day with data for this station, so break out
            break
        else:
            writeNaNColumn(outputTmpList,lineindex)
            lineindex+=1
            iterday+=timedelta(days=1)
    return (iterday,lineindex)

def iterateOverDays(iterday,allRawInput,outputTmpList,lineindex):
    while iterday<=date.today():
        line=allRawInput.readline()
        # We have run out of data, so break
        if not line: break
        
        # Get the timestamp from the line 
        (stationDataDate,duplicate)=extractRawDataDate(line)
        # Make sure the data's date and iterday match
        if(stationDataDate==iterday and not duplicate):
            writeStationColumns(line, outputTmpList,lineindex)
        else:
            writeNaNColumn(outputTmpList,lineindex)
            
        # Go to the next day.
        iterday+=timedelta(days=1)
        lineindex+=1
    return iterday, lineindex

# Handle missing data from station's last data date until today.
def handleStationEndData(iterday,outputTmpList,lineindex):
    while iterday<=date.today():
        writeNaNColumn(outputTmpList,lineindex)
        lineindex+=1
        iterday+=timedelta(days=1)
    return iterday
    
# Do the work
def writeAllStationColumns(stationName, stationAllRawName, outputTmpList):
    epochDate=convertIsoStringToDate(START_EPOCH)        
    
    with open(stationAllRawName,"r") as allRawInput:
        # Handle this special case
        #handleStationHasDataBeforeEpoch(epochDate,allRawInput,outputTmpList)
        
        # Write out initial data
        
        lineindex=1 # Start at 1 since line 0 is the header 
        stationStartDate=extractRawDataDate(allRawInput.readline())[0]
        
        (iterday,lineindex)=handlePriorToStationStartDate(epochDate,stationStartDate,outputTmpList,lineindex)
        
        # We are ready to start writing data.  Note we expect to break out of this while
        # before iterday==today.
        # Reset raw date file readline to 0
        allRawInput.seek(0)
        (iterday,lineindex)=iterateOverDays(iterday,allRawInput,outputTmpList,lineindex)

        # We have passed the end of data for the station, so fill the remaining days up to today
        # with NaN lines.  This may not be executed.
        handleStationEndData(iterday, outputTmpList,lineindex)
    
    #close and return
    allRawInput.close()
    return

#--------------------------------------------------
# Below is the actual execution
#--------------------------------------------------
eval_dir_path = properties('eval_path')

# Loop over each data set
for dataSet in os.listdir(eval_dir_path):
    projectDir=eval_dir_path+"/"+dataSet
    if(os.path.isdir(projectDir)):
        projectAllStationsOutputDir=BASE_OUTPUT_DIR+"/"+dataSet
        # Make the directory to hold the output if necessary
        if(not os.path.isdir(projectAllStationsOutputDir)): os.makedirs(projectAllStationsOutputDir)
        
        # This is a list to store the lines of the file.
        outputTmpList=[]
        writeTimeColumn(outputTmpList)
        # Loop over station directories
        for stationDir in os.listdir(projectDir):
            if (os.path.isdir(projectDir+"/"+stationDir)):
                stationName=getStationName(stationDir)
                try:
                    stationAllRawName=getStationAllRawFile(projectDir+"/"+stationDir)
                    appendColumnHeadings(stationName,outputTmpList)
                    writeAllStationColumns(stationName,stationAllRawName,outputTmpList)
                except Exception, e:
                    print "Something is screwy: ", e
                    pass

        # Write the station values list to the appropriate file.
        allInputFileName=projectAllStationsOutputDir+"/"+"all_stations.all.input"        
        with open(allInputFileName,"w") as allInputFile:
            for line in outputTmpList:
                allInputFile.write(line+"\n")
        allInputFile.close()
        del outputTmpList
