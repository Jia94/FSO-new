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
import subprocess
import os
import shutil
import sys


from china_common import currentTime, downloadHour, startHour, fcRange, \
                       STATIC_DIR, workingPathDir, offset_hr, offset_min, \
                       clean_up_dirs, WPS_namelist, WRF_namelist, cleanDirDays, \
                       gfsProdDirPrefix, gfsFileCommName, wrfbdy_typicalSize, \
                       gfs_typicalSize, num_metgrid_levels, num_metgrid_soil_levels, \
                       deadline_hours, WPS_ROOT, WRF_ROOT, GFS_DIR, ob_path, \
                       wrfinput_typicalSize, wait_file, rc_path, fc_path, \
                       gfsRange, gfsInterval, fcInterval, check_large_file, LITTLE_R_DIR

currDate = datetime.utcnow()
killTime = currDate + timedelta(hours=deadline_hours)  # The time on which the job will be killed

if "CURR_DATE" in os.environ:
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13]))  - timedelta(hours=6)

if currDate.strftime("%H%M") in currentTime  :   # It is time to play
        print 'Current time is %s' % currDate.strftime("%Y%m%d%H%M")

	index = currentTime.index(currDate.strftime("%H%M"))   # Used to locate the index of download Hour

        #if index == 0 or index == 2: adj=-5
        #if index == 1 or index == 3: adj=-7
	#runStartDate = currDate + timedelta(hours=adj) + timedelta(minutes=-10)   # Since we start the job on XX:57m
	runStartDate = currDate
	runEndDate = runStartDate + timedelta(hours=gfsRange-3) #  Let WPS decode all grib files
	ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
	hh = runStartDate.strftime("%H") 
	ccyy = runStartDate.strftime("%Y") 

        # Create the working path for this time
	ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
	workingPath = workingPathDir + '/' + ccyymmddhh

	# Create and enter working directory

        os.umask(022)                      # Let other users to read the file

        try:
	   os.makedirs(workingPath)
        except:
           pass

	os.chdir(workingPath)
	print " Create working directory %s" % workingPath

        ######################
	# Copy prepbufr file #
        ######################

        ob_path_dir = os.path.join(ob_path, ccyymmddhh)

	if not os.path.exists(ob_path_dir):
           try:
	      os.makedirs(ob_path_dir)
           except:
              pass

        # bufr_file = os.path.join(GFS_DIR,'gfs.'+ccyymmddhh, 'gfs.t' + hh + 'z.prepbufr.nr')
        ascii_file = os.path.join(LITTLE_R_DIR, ccyymmddhh, 'ob.ascii')

        # if os.path.exists(bufr_file) and os.path.getsize(bufr_file) > 10000000 :
           # shutil.copy(bufr_file, os.path.join(ob_path_dir, "ob.bufr")) 
           # print ( "Copied %s to ob.bufr ") % bufr_file
        # else :
           # print (" Can not find bufr file: %s" % bufr_file)

        print ascii_file
        if os.path.exists(ascii_file):
           # Cut off window is +/- 5 minutes
           beg_time = runStartDate + timedelta(minutes=-60)
           end_time = runStartDate + timedelta(minutes=+60)

	   obsDir = workingPath + '/obsproc'
	   if not os.path.exists(obsDir):
              try:
	           os.makedirs(obsDir)
              except:
                   pass
           os.chdir(obsDir)

           ascii_file = os.path.join(LITTLE_R_DIR, ccyymmddhh, 'ob.ascii')
           shutil.copy(ascii_file, "obs."+ccyymmddhh) 

           # Run obsproc
           sccyy = beg_time.strftime("%Y")
           smm = beg_time.strftime("%m")
           sdd = beg_time.strftime("%d")
           shh = beg_time.strftime("%H")
           snn = beg_time.strftime("%M")
           cccyy = runStartDate.strftime("%Y")
           cmm = runStartDate.strftime("%m")
           cdd = runStartDate.strftime("%d")
           chh = runStartDate.strftime("%H")
           cnn = runStartDate.strftime("%M")
           eccyy = end_time.strftime("%Y")
           emm = end_time.strftime("%m")
           edd = end_time.strftime("%d")
           ehh = end_time.strftime("%H")
           enn = end_time.strftime("%M")

           outfile = open('namelist.obsproc', "w")
           with open(os.path.join(STATIC_DIR, 'namelist.obsproc'), 'r') as infile:
               for line in infile:
                   outfile.write(line.replace("sccyy", sccyy).replace("eccyy", eccyy). \
			                            replace("ccyy", cccyy).replace("smm", smm).replace("emm", emm). \
			                            replace("mm", cmm).replace("sdd", sdd).replace("edd", edd). \
			                            replace("dd", cdd).replace("shh", shh).replace("ehh", ehh). \
			                            replace("hh", chh).replace("snn", snn).replace("enn", enn). \
			                            replace("nn", cnn))
           outfile.close()

           shutil.copyfile(os.path.join(STATIC_DIR, "obserr.txt"), \
		                        "obserr.txt")

           os.system(os.path.join('/', 'WRFDA', 'var', 'obsproc', 'obsproc.exe'))
           
		#if os.path.exists( os.path.join('/', 'obs_gts_'+cccyy+'-'+cmm+'-'+cdd+'_'+chh+':00:00.3DVAR') ):
           shutil.copy('obs_gts_'+cccyy+'-'+cmm+'-'+cdd+'_'+chh+':00:00.3DVAR', os.path.join(ob_path_dir, "ob.ascii")) 
           print ( "Copied %s to %s ob.ascii ") % ('obs_gts_'+cccyy+'-'+cmm+'-'+cdd+'_'+chh+':00:00.3DVAR', ob_path_dir)
        #else
		    #print("obsproc not success")

        sys.exit(0)

        ##################################################################################
        # Delete the depth 1 level directories under workingPath, which are 4 days older.#
        ##################################################################################

#       clean_up_dirs (workingPathDir,1,cleanDirDays)

