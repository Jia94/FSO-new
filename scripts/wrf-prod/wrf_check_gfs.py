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

if "CURR_DATE" in os.environ:
   currDate = datetime(int(os.environ['CURR_DATE'][0:4]), int(os.environ['CURR_DATE'][4:6]), \
		       int(os.environ['CURR_DATE'][6:8]), int(os.environ['CURR_DATE'][9:11]), \
		       int(os.environ['CURR_DATE'][11:13])) - timedelta(hours=6)

print "Processing :", currDate
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

	if os.path.exists(workingPath):
           shutil.rmtree(workingPath)

        try:
	   os.makedirs(workingPath)
        except:
           pass

	os.chdir(workingPath)
	print " Create working directory %s" % workingPath

        ###############
	# Running WPS #
        ###############

        # Create and enter the wps working directory

	wpsDir = workingPath + '/wps'
	if not os.path.exists(wpsDir):
           try:
		os.makedirs(wpsDir)
           except:
                pass
	os.chdir(wpsDir)

        # Check if the gfs data is available

        for file in fileList :
           print "Checking %s" % file
           while not os.path.exists(file) or os.path.getsize(file) < gfs_typicalSize :
                 print "Can not found %s" %  file
                 sys.exit(1)
                   
#       clean_up_dirs (workingPathDir,1,cleanDirDays)

