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
           while not os.path.exists(file) or os.path.getsize(file) < gfs_typicalSize :
                    print " Can not find %s" %  file
                    exit(1)
                   
        # Link the gfs data to wps working directory

        try: 
            count=0
            for a in ascii_uppercase:
                for b in ascii_uppercase:
                    for c in ascii_uppercase:
                        os.symlink(fileList[count], 'GRIBFILE.' + a + b + c)
                        count += 1
                        if count == len(fileList) : exit
        except:
            pass

        # Link the geogrid data to wps working directory

	if os.path.islink("geo_em.d01.nc"): os.remove("geo_em.d01.nc")

        try :
		os.symlink(os.path.join(STATIC_DIR, 'geo_em.d01.nc'), "geo_em.d01.nc")
        except:
		print 'Can not link %s ' % os.path.join(STATIC_DIR,'geo_em.d01.nc')
		sys.exit(1)

        # Link the Vtable

        # ''' obsolete section ?
	# if os.path.islink("Vtable"): os.remove("Vtable")
        # if os.path.getsize(os.path.join(GFS_DIR,ccyy,'gfs.'+ccyymmddhh,fileList[0])) > 150000000 :   # for 0p25 GFS data
           # try :
	   	# os.symlink(os.path.join(STATIC_DIR, 'Vtable.GFS'), "Vtable")
           # except:
		# print 'Can not link %s ' % os.path.join(STATIC_DIR,'Vtable.GFS')
        # else :   # for 0p5 GFS data
           # try :
	   	# os.symlink(os.path.join(STATIC_DIR, 'Vtable.GFS_0p5'), "Vtable")
           # except:
		# print 'Can not link %s ' % os.path.join(STATIC_DIR,'Vtable.GFS_0p5')
        # '''
	os.symlink(os.path.join(STATIC_DIR, 'Vtable.GFS'), "Vtable")
	#os.symlink(os.path.join(STATIC_DIR, 'Vtable.GFS_0p5'), "Vtable")

        # Creat the namelist.wps

	WPS_namelist (STATIC_DIR + '/' + "namelist.wps", "namelist.wps", runStartDate, runEndDate, gfsInterval)

        # ungrib GFS data

        try :
		#subprocess.call([os.path.join(WPS_ROOT, "ungrib.exe")])
                rc = bash_cmd ( [os.path.join(WPS_ROOT, "ungrib.exe")] )
                if not rc == 0: sys.exit(1)
        except :
		            print ' ungrib.exe fail to run'
		            sys.exit(1)

        # Wait for the met_em files 
        #if pd_found :
        #   time_seq = pd.date_range(runStartDate, runEndDate, freq=str(gfsInterval)+'H') 
        #elif '1.11' in np.__version__ :
        #   time_seq = np.arange(runStartDate, runEndDate, timedelta(hours=gfsInterval)).astype(datetime)
        #else:
        #   time_seq = [runStartDate + timedelta(hours=x) for x in range(0, gfsRange, gfsInterval)]
        time_seq = [runStartDate + timedelta(hours=x) for x in range(0, gfsRange, gfsInterval)]

        for time_step in time_seq :
            ungrib_file = 'GFS:' + time_step.strftime("%Y") + '-' + time_step.strftime("%m") + '-' + time_step.strftime("%d") + '_' +  time_step.strftime("%H")
            wait_file(ungrib_file)

        # Link the metgrid

	if os.path.islink("metgrid"): os.remove("metgrid")

        try :
		os.symlink(STATIC_DIR, 'metgrid')
        except:
		print 'Can not link %s ' % os.path.join(WPS_ROOT,'metgrid')
		sys.exit(1)
        
        # Link the QNWFA_QNIFA_Monthly_GFS

	if os.path.islink("QNWFA_QNIFA_Monthly_GFS"): os.remove("QNWFA_QNIFA_Monthly_GFS")

        try :
		os.symlink(os.path.join(STATIC_DIR, 'QNWFA_QNIFA_Monthly_GFS'),'QNWFA_QNIFA_Monthly_GFS')
        except:
		print 'Can not link %s ' % os.path.join(WPS_ROOT,'QNWFA_QNIFA_Monthly_GFS')
		sys.exit(1)

        # Run metgrid

        try :
		os.symlink(os.path.join(WPS_ROOT, "metgrid.exe"), "metgrid.exe")
                if 'cma' in os.uname()[1] :
		   os.system(RUN_CMD + os.path.join(WPS_ROOT, "metgrid.cmd"))
                elif 'yslogin' in os.uname()[1] :
		   os.system(RUN_CMD + ' < ' +  os.path.join(WPS_ROOT, "metgrid.cmd.lsf"))
                else:
                   rc = bash_cmd(RUN_CMD + ["metgrid.exe"])
                   if not rc == 0: sys.exit(1)
        except:
		print 'metgrid.exe fail to run'
		sys.exit(1)

        # Wait for the met_em files 
        for time_step in time_seq :
            met_file = 'met_em.d01.' + time_step.strftime("%Y") + '-' + time_step.strftime("%m") + '-' + time_step.strftime("%d") + '_' +  time_step.strftime("%H") + ':00:00.nc'
            wait_file(met_file)


#       clean_up_dirs (workingPathDir,1,cleanDirDays)

