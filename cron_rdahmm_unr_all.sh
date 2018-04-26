#!/bin/bash

/gpsData/PythonRDAHMM/cron_rdahmm_unr.py
/gpsData/PythonRDAHMM/unr_splice.py
/gpsData/PythonRDAHMM/rdahmm_eval_single.py UNR_SPLICE
/gpsData/PythonRDAHMM/create_summary_xmls.py UNR_SPLICE
/gpsData/PythonRDAHMM/create_summary_jsons.py UNR_SPLICE
