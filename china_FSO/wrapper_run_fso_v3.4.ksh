#!/bin/ksh 
#########################################################################
# Script: da_run_suite_wrapper.ksh
#
# Purpose: Provide user-modifiable interface to da_run_suite.ksh script.
#
# Description:
#
# Here are a few examples of environment variables you may want to 
# change in da_run_suite_wrapper.ksh:
#
# 1) "export RUN_WRFVAR=true" runs WRFVAR.
# 2) "export REL_DIR=$HOME/trunk" points to directory
# containing all code (I always include all components as subdirectories
# e.g. $REL_DIR/wrfvar contains the WRFVAR code.
# 3) "export INITIAL_DATE=2003010100" begins the experiment at 00 UTC
# 1 January 2003.

# You will see a more complete list of environment variables, and their default
# values in da_run_suite.ksh. If one is hard-wired (i.e. not an environment
# variable) then email wrfhelp@ucar.edu and we will add it for the next
# release.
#########################################################################
#set echo 

# 1 Decide which stages to run (run if true):
#--------------------------------------------
if [[ $1 == 'da' ]]; then
   echo ' Step 1: run assimilation at initial time (parallel) '
   # run assimilation at final time if verifying against own analysis (parallel)
   export RUN_WRFVAR=true
   export RUN_UPDATE_BC=true
   export RUN_NL_ONLY=false
   export RUN_ADJ_ONLY=false

elif [[ $1 == 'nl' ]]; then
   echo ' Step 2: run WRF Non-Linear (parallel)'
   export RUN_ADJ_SENS=true
   export RUN_NL_ONLY=true
   export RUN_ADJ_ONLY=false
   export CALCULATE_FCERR=false
   export CALCULATE_FORCING=false

elif [[ $1 == 'forcing' ]]; then
   echo ' Step 2.1: Calculating adjoint forcing (ncl) ' 
   export RUN_ADJ_SENS=true
   export RUN_NL_ONLY=false
   export RUN_ADJ_ONLY=false
   export CALCULATE_FCERR=false
   export CALCULATE_FORCING=true

elif [[ $1 == 'adj' ]]; then
   echo ' Step 2.2: run WRF adjoint Model (parallel)'
   export RUN_ADJ_SENS=true
   export RUN_ADJ_ONLY=true
   export RUN_NL_ONLY=false
   export CALCULATE_FCERR=false
   export CALCULATE_FORCING=false

elif [[ $1 == 'fcerr' ]]; then
   echo ' Step 2.3: Calculate fcerr (ncl)'
   export RUN_ADJ_SENS=true
   export RUN_NL_ONLY=false
   export RUN_ADJ_ONLY=false
   export CALCULATE_FCERR=true
   export CALCULATE_FORCING=false

elif [[ $1 == 'impact' ]]; then 
   echo ' Step 3: run adjoint of the Data Assimilation at initial time (parallel) '
   export RUN_OBS_IMPACT=true
   export NL_USE_LANCZOS=true
   export NL_WRITE_LANCZOS=true
   export NL_EPS=1E-5

else
   echo ' Unknown option, must be either da, adj or impact'
   exit
fi

# 2 Experiment details:
#-----------------------------
export CLEAN=false
export CYCLING=false
export CYCLE_NUMBER=0

# 3 Job details:
#------------------------
export NUM_PROCS=$4
#export NPN=4
#export RUN_CMD="mpirun.lsf "
#export SUBMIT="LSF"
#export LSF_PTILE=4
#export QUEUE=small
#export LSF_EXCLUSIVE=" "
#export SUBMIT="LoadLeveller"
export SUBMIT="none"
if [[ $1 == 'fcerr' ]]; then
   export SUBMIT="none"
fi
if [[ $1 == 'forcing' ]]; then
   export SUBMIT="none"
fi
export LL_PTILE=$3
export WALLCLOCK='180:00'

# 4 Time info:
#-------------------------------
export INITIAL_DATE=$2
export FINAL_DATE=${INITIAL_DATE}
export LBC_FREQ=12
export CYCLE_PERIOD=12
export LONG_FCST_TIME_1=00
export LONG_FCST_TIME_2=06
export LONG_FCST_TIME_3=12
export LONG_FCST_TIME_4=18
export LONG_FCST_RANGE_1=12
export LONG_FCST_RANGE_2=12
export LONG_FCST_RANGE_3=12
export LONG_FCST_RANGE_4=12
export NL_INTERVAL_SECONDS=43200

# 5 Directories:
#------------------------------------
export REL_DIR=$FSO_HOME
export DAT_DIR=$REL_DIR/FSO3.4
#export WRFVAR_DIR=$REL_DIR/../WRFV3/WRFDA
export WRFVAR_DIR=$REL_DIR/WRFDA
export WRFPLUS_DIR=$REL_DIR/WRFPLUSV3
export WRFNL_DIR=$WRFPLUS_DIR
export SCRIPTS_DIR=$WRFVAR_DIR/var/scripts
export GRAPHICS_DIR=$WRFVAR_DIR/var/graphics/ncl
export BUILD_DIR=$WRFVAR_DIR/var/build

export REG_DIR=$DAT_DIR
export RUN_DIR=$REG_DIR/run
export EXP_DIR=${RUN_DIR}

export FC_DIR=${DAT_DIR}/fc
export OB_DIR=${DAT_DIR}/ob
export RC_DIR=${DAT_DIR}/rc
export BE_DIR=${DAT_DIR}/be
export DA_BACK_ERRORS=${BE_DIR}/be_d01.dat

# 6 Namelist parameters:
#--------------------------
#time_control
#domain
export NL_E_WE=340
export NL_E_SN=268
export NL_DX=15000
export NL_DY=15000
export NL_TIME_STEP=90
export NL_E_VERT=30
export NL_P_TOP_REQUESTED=5000
export NL_I_PARENT_START=1
export NL_J_PARENT_START=1
export NL_DEBUG_LEVEL=0

# physics
export NL_MP_PHYSICS=4
export NL_RA_LW_PHYSICS=1
export NL_RA_SW_PHYSICS=1
export NL_SF_SFCLAY_PHYSICS=1
export NL_SF_SURFACE_PHYSICS=1
export NL_NUM_SOIL_LAYERS=4
export NL_CU_PHYSICS=1
export NL_BL_PBL_PHYSICS=1
export NL_NUM_LAND_CAT=21
export NL_RADT=9

# WRFDA
export NL_OB_FORMAT=2
export WINDOW_START=-1 # use +-1h time window for statistics
export WINDOW_END=1
export NL_CALCULATE_CG_COST_FN=false
export NL_ORTHONORM_GRADIENT=true
export NL_NTMAX=100
export NL_CV_OPTIONS=5
#export NL_USE_SYNOPOBS=false
#export NL_USE_METAROBS=false
#export NL_USE_SHIPSOBS=false
# WRFPLUS
export DOUBLE=true

# 7 FSO options:
#---------------
export ADJ_REF=2       # 1 -> Own Analysis
                       # 2 -> NCEP Analysis (default)
		       # 3 -> Observations
export ADJ_MEASURE=4   

## setup the targeted domain
export ADJ_ISTART=96
export ADJ_IEND=282
export ADJ_JSTART=10
export ADJ_JEND=220
export ADJ_KSTART=1
export ADJ_KEND=25

export ADJ_RUN_NL=true
export ADJ_RUN_PLUS=true
export NL_TRAJECTORY_IO=true

# 8 Satellite radiances:
#--------------------------
export NL_USE_AMSUAOBS=false
export NL_USE_AMSUBOBS=false
export NL_USE_MHSOBS=false
export NL_USE_AIRSOBS=false
export NL_RTM_OPTION=2        # 1:rttov; 2:crtm
export  NL_RTMINIT_NSENSOR=0  #2 #1 # amsua, amsub, mhs
export NL_RTMINIT_PLATFORM=9  #,10 #,1,1,1,1,1,1,1
export    NL_RTMINIT_SATID=2  #,2  #,15,16,18,15,16,17,18
export   NL_RTMINIT_SENSOR=11 #,3 #,3,3,3,4,4,4,15
export NL_THINNING=true
export NL_THINNING_MESH=125.0 #,90.0

### CRTM related setup
export DA_CRTM_COEFFS=${WRFVAR_DIR}/var/run/crtm_coeffs
export NL_USE_CRTM_KMATRIX=true
export NL_WRITE_JACOBIAN=false
export NL_CRTM_CLOUD=false
export NL_CRTM_ATMOSPHERE=5 # 0=INVALID_MODEL, 1=TROPICAL, 2=MIDLATITUDE_SUMMER
                            # 3=MIDLATITUDE_WINTER, 4=SUBARCTIC_SUMMER
			    # 5=SUBARCTIC_WINTER, 6=US_STANDARD_ATMOSPHERE

### VarBC related setup
export NL_USE_VARBC=false
export NL_VARBC_FACTOR=2.0
export DA_VARBC_IN=${WRFVAR_DIR}/var/run/VARBC.in

# 9 Run Script:
#--------------------------
export SCRIPT=$SCRIPTS_DIR/da_run_suite.ksh
$SCRIPTS_DIR/da_run_job.ksh
exit 0

