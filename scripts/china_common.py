#!/usr/bin/env python

from ftplib   import FTP
import os, sys
from datetime import timedelta
import time
import shutil
from subprocess import check_call, Popen, PIPE
from subprocess import CalledProcessError, check_output

# Predefined variables
currentTime = ['0000', '1200']
currentTime_fso = ['0000', '1200']
downloadHour = ['00', '12']
startHour =    [0, 0, 0 ,0]
offset_hr = -3
offset_min = -30
cycInterval = 6
gfsRange = 51   
fcRange = 12
gfsInterval = 12 
fcInterval = 12 

#SET DIR
WPS_ROOT = "/WPS"
WRF_ROOT = "/WRFV3"
STATIC_DIR = "/gjx_static"
GFS_DIR = os.path.join('/gfs')
workingPathDir = "/gjx_working"
LITTLE_R_DIR="/little_r"
obsFileCommName = "gdas1.txxz.prepbufr.nr"
FSO_PATH = "/FSO3.4"
rc_path = os.path.join( FSO_PATH, 'rc')
fc_path = os.path.join( FSO_PATH, 'fc')
ob_path = os.path.join( FSO_PATH, 'ob')

#GFS SET
gfsRange_download = 94   # forecast Hours
ncepProdFtp = "ftpprd.ncep.noaa.gov"
gfsProdDirPrefix = "/gfs."   # Producet path prefix
gfsFileCommName = "gfs.txxz.pgrb2.0p25.fhhh"        # Product file name pattern
#gfsFileCommName = "gfs.txxz.pgrb2.0p50.fhhh"
gfs_typicalSize = 15000000
wrfbdy_typicalSize = 3200000000
wrfinput_typicalSize = 1800000000
num_metgrid_levels = '32'
num_metgrid_soil_levels = '4'
deadline_hours=6
cleanDirDays = 4

##############################################################################################
def clean_up_dirs ( directory, depth, days ):

    try :
        os.chdir(directory)
    except :
        print 'Can not change to %d' % directory
	pass
    else :

    	# How many seconds for number of days.
    	numdays = 86400*days

    	# Current time
    	now = time.time()

    	for r,d,f in os.walk('.'):
            if len(r.split('/')) == 1+depth :
                timestamp = os.path.getmtime(r)
                   # if older than days
                if now-numdays > timestamp:
                   try:
                      print " Removing ",r
                      shutil.rmtree(r)
                   except Exception,e:
                      print e
                      pass
                   else:
                      print "%s is deleted as it is older than %d days" % (r, days)

def WPS_namelist (WPSTemplate, namelist, startDate, endDate, interval_h):
    outfile = open (namelist, "w")
    sccyy = startDate.strftime("%Y")
    smm = startDate.strftime("%m")
    sdd = startDate.strftime("%d")
    shh = startDate.strftime("%H")
    eccyy = endDate.strftime("%Y")
    emm = endDate.strftime("%m")
    edd = endDate.strftime("%d")
    ehh = endDate.strftime("%H")

    with open(WPSTemplate, 'r') as infile:
         for line in infile:
             outfile.write(line.replace("sccyy",sccyy).replace("eccyy",eccyy).replace("smm",smm).replace("emm",emm).replace("sdd",sdd).replace("edd",edd).replace("shh",shh).replace("ehh",ehh).replace("run_interval",str(interval_h*3600)))
    outfile.close()

def WRF_namelist (WRFTemplate, namelist, startDate, endDate, interval_h):
    outfile = open (namelist, "w")
    delta = endDate - startDate
    RUN_HOURS = delta.seconds/3600
    print delta.seconds,RUN_HOURS
    sccyy = startDate.strftime("%Y")
    smm = startDate.strftime("%m")
    sdd = startDate.strftime("%d")
    shh = startDate.strftime("%H")
    eccyy = endDate.strftime("%Y")
    emm = endDate.strftime("%m")
    edd = endDate.strftime("%d")
    ehh = endDate.strftime("%H")

    dfiStartDate = startDate + timedelta(minutes=-20)
    dfiEndDate = startDate + timedelta(minutes=10)

    dfisyear = dfiStartDate.strftime("%Y")
    dfismonth = dfiStartDate.strftime("%m")
    dfisday = dfiStartDate.strftime("%d")
    dfishour = dfiStartDate.strftime("%H")
    dfismin = dfiStartDate.strftime("%M")
    dfieyear = dfiEndDate.strftime("%Y")
    dfiemonth = dfiEndDate.strftime("%m")
    dfieday = dfiEndDate.strftime("%d")
    dfiehour = dfiEndDate.strftime("%H")
    dfiemin = dfiEndDate.strftime("%M")

    with open(WRFTemplate, 'r') as infile:
         for line in infile:
             outfile.write(line.replace("sccyy",sccyy).replace("eccyy",eccyy).replace("smm",smm).replace("emm",emm).replace("sdd",sdd).replace("edd",edd).replace("shh",shh).replace("ehh",ehh).replace("RUN_HOURS",str(RUN_HOURS)).replace("NUM_METGRID_SOIL_LEVELS",num_metgrid_soil_levels).replace("NUM_METGRID_LEVELS",num_metgrid_levels).replace("dfisyear",dfisyear).replace("dfieyear",dfieyear).replace("dfismonth",dfismonth).replace("dfiemonth",dfiemonth).replace("dfisday",dfisday).replace("dfieday",dfieday).replace("dfishour",dfishour).replace("dfiehour",dfiehour).replace("dfismin",dfismin).replace("dfiemin",dfiemin).replace("run_interval",str(interval_h*3600)))
    outfile.close()


def ftp_download ( ftpSite, ftpDir, fileList, destDir, typicalSize ) :
    
    # Start to download files
    try:
        ftp = FTP(ftpSite)             # connect to host, default port
    except:
        print 'Cannot connect to %s' % ftpSite
        return False
    else:
        try:
            ftp.login()                     # user anonymous, passwd anonymous@
        except:
            print 'Cannot log in with anaonymous'
            return False
        else:
            try:
                ftp.cwd('/pub'+ftpDir)                # Change to destination
            except:
                print 'Cannot change to directory %s ' % '/pub'+ftpDir
                ftp.quit()
                return False
            else:
                for fileName in fileList : 
                    done = False
                    now_size = -9999
                    print "Looking for %s ..." % fileName
                    while not done:
                       size = now_size   # Found the file
                       tmp = os.popen('curl -sI ' + ftpSite + ftpDir + '/' + fileName + ' | grep Content-Length | cut -d\' \' -f2 | tr -d \"\\015\"')
                       ttmp = tmp.read()
                       if ttmp : 
                          now_size = int(ttmp)
                          print "Found the file %s with size of %d" % (fileName, now_size)
                       if size == now_size and now_size > typicalSize :
                          done = True 
                          print '%s with size %d, is ready to be downloaded' % (fileName, size )
                       else:
                          time.sleep(5)

 
                    #  Check if the file is already existed
                    if os.path.exists(destDir+'/'+fileName) and os.path.getsize(destDir+'/'+fileName) == size :
                        print os.path.join(destDir, fileName) + " is existed "
                        continue          # skip to next file donwloading
                        
                    # start to download the file
                    done = False
                    while not done:
                         try:
                             os.system('wget -O ' + '.' + fileName + '.tmp' + ' -e "http_proxy=http://webproxy.watson.ibm.com:8080/" -c -t0 -T120 -w20 ' + ftpSite + ftpDir + '/' +  fileName)
                         except:
                             print 'Fail to fetch %s with size %d, try again' % (fileName, size)
                             time.sleep(5)
                         else:
                             print "Downloaded %s" % (ftpSite + ":"+ ftpDir + "/" + '.' + fileName + '.tmp')
                             shutil.move('.' + fileName + '.tmp', fileName)
                             done =True                                      # Tell this file is donwloaded.

                ftp.quit()    # ftp quit
                return True



def check_large_file(file):
   try:
       check_call(["ls", file])
       df =  Popen(["ls", file], stdout=PIPE)
       output, err = df.communicate()
   except Exception as error:
       return False
   return True

def wait_file( file ) :
        notfound = True
        while notfound:
              #if os.path.exists(file):
              if check_large_file(file):
                 print ' Found ', file
                 notfound = False
                 exit
              time.sleep(5)

def bash_cmd(cmd):
    print cmd
    try:
        output = check_output(cmd)
        returncode = 0
    except CalledProcessError as e:
        output = e.output
        returncode = e.returncode

    return returncode
