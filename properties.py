#!/usr/local/bin/python
#==========================================================================
# Definitions of all global variables such as paths and commands used by
# data processing scripts. Imported and invoked internally. 
#
#===========================================================================

def properties(key):
    V={}
    V['cron_path']="/gpsData/RDAHMM/CRON_Download/"  
    V['download_path']="/gpsData/RDAHMM/Download/"  
    V['script_path']="/gpsData/PythonRDAHMM/"
    V['data_path']="/gpsData/RDAHMM/Data/"
    # temp_path is the temporary working directory for ingesting raw data
    V['temp_path']="/gpsData/RDAHMM/TEMP/"
    V['model_path']="/gpsData/RDAHMM/Model/"
    V['eval_path']="/var/www/html/daily_rdahmmexec/daily/"
    V['train_epoch']="2013-12-31"
    V['rdahmm_bin']="/gpsData/RDAHMM/rdahmm3/bin/rdahmm"
    V['rdahmm_model_parm']="-data <inputFile> -T <dataCount> -D <dimensionCount> -N 5 -output_type gauss -anneal -annealfactor 1.1 -betamin 0.1 -regularize -omega 0 0 1 1.0e-6 -ntries 10 -seed 1234"
    V['rdahmm_eval_parm']="-data <proBaseName>.all.input -T <dataCount> -D <dimensionCount> -N 5 -output_type gauss -A <modelBaseName>.A -B <modelBaseName>.B -pi <modelBaseName>.pi -minvalfile <modelBaseName>.minval -maxvalfile <modelBaseName>.maxval -rangefile <modelBaseName>.range -eval"
    V['dygraphsJs']="/gpsData/PythonRDAHMM/dygraphsJsCreator.perl"
    return V[key]
