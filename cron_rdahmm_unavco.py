#!/usr/local/bin/python
#==========================================================================
# Ingest, and execute rdahmm evaluation for unavco datasets 
# Set up a cron job to run nightly
#
# usage: cron_rdahmm_unavco.py
#
#===========================================================================
import os, subprocess, sys
from threading import Thread
from properties import properties

unavco_cmd = properties('script_path') + "/unavco_ingest_single.py"
eval_cmd = properties('script_path') + "/rdahmm_eval_single.py"
xml_cmd = properties('script_path') + "/create_summary_xmls.py"
json_cmd = properties('script_path') + "/create_summary_jsons.py"

class ThreadJob(Thread):

    def __init__(self, dataset):
        Thread.__init__(self)
        self.source = dataset
        self.dataset = "UNAVCO_" + dataset.upper()

    def run(self):
	# ingest a given dataset: pbo | nucleus
        print "+++Starting process UNAVCO ", self.source, " ..."
        cmd = unavco_cmd
        p = subprocess.Popen([cmd, self.source], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        if p.returncode != 0:
            print p.stderr  
        print "+++Finished process UNAVCO ", self.source
     
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

for dataset in ['pbo', 'nucleus']:
    t = ThreadJob(dataset)
    t.start()
