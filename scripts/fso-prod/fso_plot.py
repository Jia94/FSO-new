#!/usr/bin/env python
""" This is the job control script"""

# Import modules
from ftplib   import FTP
from datetime import datetime, timedelta
from string import ascii_uppercase
try:
    import numpy as np
    np_found = True
except ImportError:
    np_found = False
import resource
import os
import shutil
import sys

from china_common import currentTime_fso

if 'yslogin' in os.uname()[1]:
   print ' Running system on yellowstone '
elif 'cma' in os.uname()[1]:
   print ' Running system on IBM CMA '
   os.environ['NCARG_ROOT']='/cma/u/app/ncl'
   os.environ['PATH']='/cma/u/app/ncl-6.1.2/bin:'+os.environ['PATH']
else:
   print " Running system on customers machine "
#  os.environ['NCARG_ROOT']='/usr'

sys.stdout.flush()

FSO_PATH="/home/zwtd/FSO/china_FSO"

currDate = datetime.utcnow()
#killTime = currDate + timedelta(hours=deadline_hours)  # The time on which the job will be killed

if "CURR_DATE" in os.environ:
   print 'Found CURR_DATE'
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13])) - timedelta(hours=20)

if currDate.strftime("%H%M") in currentTime_fso  :   # It is time to play
   print 'Current time is %s' % currDate.strftime("%Y%m%d%H%M")

   sys.stdout.flush()

   index = currentTime_fso.index(currDate.strftime("%H%M"))
   # if index == 0 or index == 2: adj=-29
   # if index == 1 or index == 3: adj=-31
   # runStartDate = currDate + timedelta(hours=adj) + timedelta(minutes=-40)   # Since we start the job on XX:57m
   runStartDate = currDate
   ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
   print 'Processing time is %s' % ccyymmddhh

# --------------- modified by liujingwei start -----------------
   try:
      plots_dir = os.path.join(FSO_PATH, 'run', ccyymmddhh, 'fsoplots')
      os.makedirs( plots_dir )
   except:
      pass
   os.chdir(os.path.join(FSO_PATH, 'fsoplot'))
   print(os.path.join(FSO_PATH, 'fsoplot'))
   sys.stdout.flush()
   with open('timepath.py', 'w') as f:
       f.write("#!/usr/bin/python\n")
       f.write("filename='../run/"+ccyymmddhh+"/obsimpact/gts_omb_oma_01'\n")
       f.write("datatime='"+ccyymmddhh+"'")
   os.system("python3 ./data2pg.py")
   os.system("python3 ./drawfso.py")
   os.system("mv *.png " + plots_dir)   
   # with open('ftp.sh','w') as f1:
       # f1.write("#!/bin/sh")
       # f1.write("ftp -n<<!")
       # f1.write("open 10.36.5.24")
       # f1.write("user aaa passwd")
       # f1.write("prom")
       # f1.write("bin")
       # f1.write("cd /ddd/ddd/")
       # f1.write("mkdir "+ccyymmddhh)
       # f1.write("cd "+ccyymmddhh)
       # f1.write("mput *.png")
       # f1.write("bye")
       # f1.write("!")
   # os.system("chmod +x ftp.sh")
#   os.system ( 'ncl \'start_date="' + ccyymmddhh + '"\'' + ' plot_levs_impact.ncl')
#   os.system ( 'ncl \'start_date="' + ccyymmddhh + '"\'' + ' plot_avg_obs_impact.ncl')
#   os.system ( 'ncl \'start_date="' + ccyymmddhh + '"\'' + ' plot_gts_omb_oma.ncl')
#   os.system ( 'ncl \'start_date="' + ccyymmddhh + '"\'' + ' plot_impact_by_obsnum_time_average.ncl')
#   os.system(' mv *.pdf '+ plots_dir )
# ---------------- modified by liujingwei end ------------------
   sys.exit(0)

#  clean_up_dirs (workingPathDir,1,cleanDirDays)
