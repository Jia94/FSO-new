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


#scriptpath = "/glade/scratch/xinzhang/GUOJX/newcode/gjx_common"
#scriptpath = "/cmb/g5/guojx/newcode/gjx_common"
#sys.path.append(os.path.abspath(scriptpath))

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

        ######################
      	# Check Obser.  file #
        ######################

        ob_path_dir = os.path.join(ob_path, ccyymmddhh)

      	if not os.path.exists(ob_path_dir):
           try:
	      os.makedirs(ob_path_dir)
           except:
              pass

        obs_file = os.path.join(ob_path_dir, "ob.ascii")

        if os.path.exists(obs_file) :

           print ( "observation file : %s is available ") % obs_file

        else :

           obs_file = os.path.join(ob_path_dir, "ob.bufr")

           if os.path.exists(obs_file) and os.path.getsize(obs_file) > 1000000 :
              print ( "observation file : %s is available ") % obs_file
           else :
              print (" Can not find obs file: %s" % obs_file)
              sys.exit(1)

        sys.exit(0)

#       clean_up_dirs (workingPathDir,1,cleanDirDays)
	

