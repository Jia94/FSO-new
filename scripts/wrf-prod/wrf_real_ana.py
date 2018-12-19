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


print 'pandas is found ? ', pd_found
if np_found:
   print 'numpy version is :', np.__version__

if 'yslogin' in os.uname()[1] :
   print ' Running system on yellowstone '
   os.environ['LSF_BINDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/bin'
   os.environ['LSF_SERVERDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/etc'
   os.environ['LSF_LIBDIR']='/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/lib'
   os.environ['LSF_ENVDIR']='/ncar/opt/lsf/conf'
   RUN_CMD = "/ncar/opt/lsf/9.1/linux2.6-glibc2.3-x86_64/bin/bsub "
elif 'cma' in os.uname()[1] :
   print ' Running system on IBM CMA '
   RUN_CMD = "/usr/bin/llsubmit "
else:
   print " Running system on customers machine "
   RUN_CMD = ["/usr/local/bin/mpirun", "-np", os.environ['NPE']]
   

sys.stdout.flush()

#scriptpath = "/glade/scratch/xinzhang/GUOJX/newcode/gjx_common"
#scriptpath = "/scif/apps/wrf/bin"
#sys.path.append(os.path.abspath(scriptpath))

from china_common import currentTime, downloadHour, startHour, fcRange, \
                       STATIC_DIR, workingPathDir, offset_hr, offset_min, \
                       clean_up_dirs, WPS_namelist, WRF_namelist, cleanDirDays, \
                       gfsProdDirPrefix, gfsFileCommName, wrfbdy_typicalSize, \
                       gfs_typicalSize, num_metgrid_levels, num_metgrid_soil_levels, \
                       deadline_hours, WPS_ROOT, WRF_ROOT, GFS_DIR, ob_path, \
                       wrfinput_typicalSize, wait_file, rc_path, fc_path, bash_cmd, \
                       gfsRange, gfsInterval, fcInterval, check_large_file, LITTLE_R_DIR

currDate = datetime.utcnow()
killTime = currDate + timedelta(hours=deadline_hours)  # The time on which the job will be killed

if "CURR_DATE" in os.environ:
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13])) - timedelta(hours=6)

if currDate.strftime("%H%M") in currentTime  :   # It is time to play
        print 'Current time is %s' % currDate.strftime("%Y%m%d%H%M")

	index = currentTime.index(currDate.strftime("%H%M"))   # Used to locate the index of download Hour

    #    if index == 0 or index == 2: adj=-5
    #    if index == 1 or index == 3: adj=-7
	#runStartDate = currDate + timedelta(hours=adj) + timedelta(minutes=-10)   # Since we start the job on XX:57m
	runStartDate = currDate
	runEndDate = runStartDate + timedelta(hours=gfsRange-3) #  Let WPS decode all grib files
	ccyymmddhh = runStartDate.strftime("%Y%m%d%H") 
	hh = runStartDate.strftime("%H") 
	ccyy = runStartDate.strftime("%Y") 

        # Construct the path and file names to download
	gfsProdDir = gfsProdDirPrefix + ccyymmddhh
	fileList = [ gfsFileCommName.replace('xx',downloadHour[index]).replace('hhh',str(i).zfill(3)) for i in xrange(0,gfsRange,gfsInterval) ]

        # Prepend the GFS path
        fileList = [ os.path.join(GFS_DIR, 'gfs.'+ccyymmddhh, file) for file in fileList ]

        print 'Model start time is %s' % runStartDate.strftime("%Y%m%d%H%M")
        print 'Model   end time is %s' % runEndDate.strftime("%Y%m%d%H%M")

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

        wpsDir = workingPath + '/wps'

        ############################
	# Running REAL to creat RC #
        ############################

        # Creat and enter the wrf working firectory

	realDir = os.path.join(workingPath,'real')
	if not os.path.exists(realDir):
		os.makedirs(realDir)
	os.chdir(realDir)

        # Link all DATA for wrf run

	for file in os.listdir(os.path.join(WRF_ROOT, "run")):
 		if file.endswith("DATA"):
    			if os.path.islink(file): os.remove(file)
    			os.symlink(os.path.join(WRF_ROOT, "run", file), file)

        # Link all TBL for wrf run

	for file in os.listdir(os.path.join(WRF_ROOT, "run")):
 		if file.endswith("TBL"):
    			if os.path.islink(file): os.remove(file)
    			os.symlink(os.path.join(WRF_ROOT, "run", file), file)

        # Link all BIN for wrf run

        for file in os.listdir(os.path.join(WRF_ROOT, "run")):
                if file.endswith("BIN"):
                        if os.path.islink(file): os.remove(file)
                        os.symlink(os.path.join(WRF_ROOT, "run", file), file)

        # Link the first time met_em file

	for file in os.listdir(wpsDir):
 		if file.startswith("met_em"):
    			if os.path.exists(file): os.remove(file)
    			os.symlink(os.path.join(wpsDir, file), os.path.join(realDir,file))

        # Create namelist.input

	WRF_namelist (os.path.join(STATIC_DIR, "namelist.input"), "namelist.input", runStartDate, runStartDate, fcInterval)

        # run real.exe to get the wrfinput_d01 only

        os.symlink(os.path.join(WRF_ROOT, "main", "real.exe"), "real.exe")
        if 'cma' in os.uname()[1] :
	    os.system(RUN_CMD + os.path.join(WRF_ROOT,"main","real.cmd" ))
        elif 'yslogin' in os.uname()[1] :
	    os.system(RUN_CMD + ' < ' + os.path.join(WRF_ROOT,"main","real.cmd.lsf" ))
        else:
            rc=bash_cmd(RUN_CMD + ["real.exe"])
            if not rc == 0: sys.exit(1)

        wait_file("wrfinput_d01")

        # Save the file to rc path
        rc_path_dir = os.path.join(rc_path, ccyymmddhh)

        try:
	   os.makedirs(rc_path_dir)
        except:
           pass

        shutil.move("wrfinput_d01", os.path.join(rc_path_dir, "wrfinput_d01.ana")) 
        print (" Save wrfinput_d01.ana at  %s ") % rc_path_dir


#       clean_up_dirs (workingPathDir,1,cleanDirDays)

