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


print 'pandas is found ? ', pd_found
if np_found:
   print 'numpy version is :', np.__version__

if 'yslogin' in os.uname()[1]:
   print ' Running system on yellowstone '
   os.environ['LSF_BINDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/bin'
   os.environ['LSF_SERVERDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/etc'
   os.environ['LSF_LIBDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/lib'
   os.environ['LSF_ENVDIR']='/ncar/opt/lsf/conf'
   RUN_CMD = "/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/bin/bsub "
elif 'cma' in os.uname()[1]:
   print ' Running system on IBM CMA '
   os.environ['NCARG']='/cma/u/app/ncl-6.1.2'
   os.environ['NCARG_ROOT']='/cma/u/app/ncl'
   os.environ['PATH']='/cma/u/app/ncl-6.1.2/bin:'+os.environ['PATH']
   RUN_CMD = "/usr/bin/llsubmit "
else:
   print " Running system on customers machine "
   os.environ['NCARG_ROOT']='/usr'


from china_common import deadline_hours, ob_path, \
                       wait_file, rc_path, fc_path, \
                       FSO_PATH, bash_cmd, currentTime_fso

currDate = datetime.utcnow()
killTime = currDate + timedelta(hours=deadline_hours)  # The time on which the job will be killed

if "CURR_DATE" in os.environ:
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13])) - timedelta(hours=20)

if currDate.strftime("%H%M") in currentTime_fso  :   # It is time to play

        print 'Current time is %s' % currDate.strftime("%Y%m%d%H%M")

        index = currentTime_fso.index(currDate.strftime("%H%M"))
        # if index == 0 or index == 2: adj=-29
        # if index == 1 or index == 3: adj=-31
      	# runStartDate = currDate + timedelta(hours=adj) + timedelta(minutes=-40)   # Since we start the job on XX:57m
      	runStartDate = currDate
      	ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
      	ccyy = runStartDate.strftime("%Y") 
      	mm = runStartDate.strftime("%m") 
      	dd = runStartDate.strftime("%d") 
      	hh = runStartDate.strftime("%H") 

        # Enter into FSO
       	os.chdir(FSO_PATH)

        ######################
      	# Run Step 3         #
        ######################

        rc = bash_cmd(["./wrapper_run_fso_v3.4.ksh", "fcerr", ccyymmddhh, '1', '1'])
        if not rc==0: sys.exit(1)

        sys.exit(0)

#       clean_up_dirs (workingPathDir,1,cleanDirDays)
	

