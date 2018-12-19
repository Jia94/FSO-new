#!/usr/bin/env python
""" This is the job control script"""

# Import modules
from ftplib   import FTP
from datetime import datetime, timedelta
from string import ascii_uppercase
import imp
try:
    imp.find_module('pandas')
    pd_found = True
    import pandas as pd
except ImportError:
    pd_found = False
import time
try:
    import numpy as np
    np_found = True
except ImportError:
    np_found = False
import os
import shutil
import sys


from china_common import deadline_hours, ob_path, \
                       wait_file, rc_path, fc_path, \
                       FSO_PATH, currentTime_fso

currDate = datetime.utcnow()
killTime = currDate + timedelta(hours=deadline_hours)  # The time on which the job will be killed

if "CURR_DATE" in os.environ:
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13])) - timedelta(hours=20)

if currDate.strftime("%H%M") in currentTime_fso  :   # It is time to play

        print 'Current time is %s' % currDate.strftime("%Y%m%d%H%M")

        index = currentTime_fso.index(currDate.strftime("%H%M"))
        #if index == 0 or index == 2: adj=-29
        #if index == 1 or index == 3: adj=-31
      	runStartDate = currDate
      	#runStartDate = currDate + timedelta(hours=adj) + timedelta(minutes=-40)   # Since we start the job on XX:57m
      	ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
      	ccyy = runStartDate.strftime("%Y") 
      	mm = runStartDate.strftime("%m") 
      	dd = runStartDate.strftime("%d") 
      	hh = runStartDate.strftime("%H") 

        #######################
      	# Check verif. rc file#
        #######################
        rc_path_dir = os.path.join(rc_path, (runStartDate + timedelta(hours=12)).strftime("%Y%m%d%H"))

      	if not os.path.exists(rc_path_dir):
           print (" RC path %s is not available ") % rc_path_dir
           sys.exit(1)

        if not os.path.exists( os.path.join(rc_path_dir, "wrfinput_d01.ana") ) : 
           print (" RC wrfinput_d01.ana is not available at %s ") % rc_path_dir 
           sys.exit(1)
        print ("Found the RC wrfinput at %s") % rc_path_dir

        sys.exit(0)

#       clean_up_dirs (workingPathDir,1,cleanDirDays)
	

