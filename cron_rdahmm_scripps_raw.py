#!/usr/local/bin/python
#==========================================================================
# Download, ingest, and execute rdahmm evaluation for updated scripps datasets 
# Set up a cron job to run nightly
#
# usage: cron_rdahmm.py
#
#===========================================================================
import os, subprocess, sys, glob
import urllib,string
import re
from threading import Thread
from properties import properties

cron_path = properties('cron_path') + "/SCRIPPS_RAW/"
download_path = properties('download_path') + "/SCRIPPS_RAW/"
#model_path = properties('model_path') 

scripps_data = properties('cron_path') + "/SCRIPPS_RAW/*.tar"
scripps_cmd = properties('script_path') + "/scripps_ingest_single_raw.py"
eval_cmd = properties('script_path') + "/rdahmm_eval_single.py"
xml_cmd = properties('script_path') + "/create_summary_xmls.py"
json_cmd = properties('script_path') + "/create_summary_jsons.py"

class ThreadJob(Thread):

    def __init__(self, tarball):
        Thread.__init__(self)
        self.tarball = tarball
        self.dataset = string.split(tarball, "/")[-1][:-13]

    def run(self):
	# ingest a given tar ball
        print "+++Starting process ", self.tarball, " ..."
        cmd = scripps_cmd
        #cmd = "echo"
        p = subprocess.Popen([cmd, self.tarball], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode != 0:
            print p.stderr  
        print "+++Finished process ", self.tarball
     
        # run rdahmm evaluation on the corresponding dataset 
        print "+++Starting process ", self.dataset, " ..."
        cmd = eval_cmd
        #cmd = "echo"
        p = subprocess.Popen([cmd, self.dataset], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode != 0:
            print p.stderr        
        print "+++Finished process ", self.dataset

        # create summary xml on the corresponding dataset 
        print "+++creating summary xml for  ", self.dataset, " ..."
        cmd = xml_cmd
        #cmd = "echo"
        p = subprocess.Popen([cmd, self.dataset], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode != 0:
            print p.stderr        
        print "+++Finished creating summary xml for  ", self.dataset

        # create summary json on the corresponding dataset 
        print "+++creating summary json for  ", self.dataset, " ..."
        cmd = json_cmd
        #cmd = "echo"
        p = subprocess.Popen([cmd, self.dataset], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode != 0:
            print p.stderr        
        print "+++Finished creating summary json for  ", self.dataset

        # update download_path with the latest tarball
        oldtars = download_path + self.dataset + "*.tar"
        if len(glob.glob(oldtars)) > 0:
            cmd = "rm " + oldtars
            os.system(cmd)
            #print cmd 
        cmd = "cp " + self.tarball + " " + download_path
        os.system(cmd)
        #print cmd

# Get current SCRIPPS raw datasets
# http://garner.ucsd.edu/pub/timeseries/measures/rawNeuTimeSeries.MEASURES_Combination.20130616.tar
# We will compare tars available on the web site with the ones we already have. 
# We will only download the differences.
def list_tars(url, username="anonymous", password="quakesim@iu.edu" ):
    """download html with login"""
    newurl = "http://" + username + ":" + password + "@" + string.replace(url,"http://","")    
    opener = urllib.urlopen(newurl)
    html = opener.read()
    opener.close()
    r = re.compile('(?<=href=").*?(?=")')
    links = r.findall(html)
    tars = [name for name in links if name[-4:]=='.tar' ]    
    return tars
        
url = 'http://garner.ucsd.edu/pub/timeseries/measures/'
tars = list_tars(url)
datasets = os.listdir(download_path)
#datasets = glob.glob(download_path + "*.tar")
#datasets = [string.split(name,"/")[-1] for name in datasets]
#print datasets

# newdatasets is the list of all updated tarballs from SCRIPPS site
newdatasets = []
for tarball in tars:
    #Currently we only want rawNeuTimeSeries
    if not "rawNeuTimeSeries" in tarball:
        continue

    if not tarball in datasets:
        newdatasets.append(tarball)

if len(newdatasets) == 0:
    sys.exit("No new scripps raw dataset available today.")
 
# clear working directory cron_path
cmd = "rm -r " + cron_path + "/*"
os.system(cmd)

wgetcmd = "wget -nv --user=anonymous --password='quakesim@iu.edu' -P " + cron_path + " -r -nH --cut-dirs=4 --no-parent --reject 'index.html*' " + url + "%s"
for tarball in newdatasets:
    cmd = wgetcmd % tarball
    #print cmd
    os.system(cmd)
    #break

#threadjobs = []

for tarball in glob.glob(cron_path + "*.tar"):
    #print tarball
    t = ThreadJob(tarball)
    t.start()
    #threadjobs.append(t)
    #sys.exit()

# wait for all processing threads to finish before generating the XML
#for t in threadjobs:
#    t.join()

# Create the summary XML command
#print "Finally, create the summary XML files."
#os.system(xml_cmd)
