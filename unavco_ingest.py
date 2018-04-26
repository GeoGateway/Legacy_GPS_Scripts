#!/usr/local/bin/python
#==========================================================================
# Ingest both PBO and NUCLEUS datasets.  
# Use subprocess to invoke unavco_ingest_single.py for parallel processing
#
# usage: unavco_ingest.py
#
#===========================================================================
import os, subprocess, sys
from threading import Thread
from properties import properties

unavco_cmd = properties('script_path') + "/unavco_ingest_single.py"

class ThreadJob(Thread):

    def __init__(self, dataset):
        Thread.__init__(self)
        self.dataset = dataset

    def run(self):
        cmd = unavco_cmd
        # start = time.time()
        print "+++Starting process UNAVCO ", dataset, " ..."
        p = subprocess.Popen([cmd, self.dataset], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        # end = time.time()
        if p.returncode != 0:
            print p.stderr        
        print "+++Finished process UNAVCO ", dataset

for dataset in ['pbo', 'nucleus']:
    t = ThreadJob(dataset)
    t.start()
