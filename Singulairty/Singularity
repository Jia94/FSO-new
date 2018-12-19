BootStrap: docker
From: weatherlab/longrun_base2_gnu

%labels
Maintainer Xin Zhang
Version v1.0

%runscript
    echo "Welcome, this is Singularity container for FSO"
    echo "Rooooar!"
    echo "Arguments received: $*"
    exec echo "$@"

%environments
    NETCDF=/usr
    WRFIO_NCD_LARGE_FILE_SUPPORT=1
    LD_LIBRARY_PATH=/usr/local/lib
    FSO_HOME=/
    PATH=.:$PATH
    NCARG_ROOT=/usr \
    ECFLOW_USER=ecflow \
    ECF_PORT=2500 \
    ECF_HOME=/home/ecflow \
    LANG=en_US.UTF-8 \
    DISPLAY=:0 \
    PYTHONPATH=/usr/local/lib/python2.7/site-packages
    export NETCDF FSO_HOME WRFIO_NCD_LARGE_FILE_SUPPORT LD_LIBRARY_PATH PATH NCARG_ROOT ECFLOW_USER ECF_PORT ECF_HOME LANG DISPLAY PYTHONPATH

%files
    WPS
    WRFV3
    WRFPLUSV3
    WRFDA

%post
    mkdir -p /FSO3.4
    mkdir -p /gjx_working
    mkdir -p /gjx_static
    mkdir -p /gfs
    mkdir -p /little_r

    ulimit -s unlimited

    # Build WRF

    cd /WRFV3
    cp configure.wrf.gnu configure.wrf
    ./compile em_real

    # Build WPS

    cd /WPS
    cp configure.wps.gnu configure.wps
    ./compile
    find . -name "*.o" -delete
    find . -name "*.mod" -delete
    find . -name "*.a" -delete

    # clean up WRF

    cd /WRFV3
    find . -name "*.o" -delete
    find . -name "*.mod" -delete
    find . -name "*.a" -delete

    # Build WRFPLUS
    
    cd /WRFPLUSV3
    cp configure.wrf.gnu configure.wrf
    ./compile em_real
    find . -name "*.o" -delete
    find . -name "*.mod" -delete
    find . -name "*.a" -delete

    # Build WRFDA

    cd /WRFDA
    cp configure.wrf.gnu configure.wrf
    ./compile all_wrfvar
    find . -name "*.o" -delete
    find . -name "*.mod" -delete
    find . -name "*.a" -delete

##############################
# Build real time wrf
##############################

%apphelp wrf
    This is the help for real time wrf.

%appfiles wrf
    run_real_time.bash bin/run_real_time.bash

%appinstall wrf
    chmod +x bin/run_real_time.bash

%apprun wrf
    echo "RUNNING REAL-TIME WRF"
    run_real_time.bash

##############################
# Build fso
##############################

%apphelp fso
    This is the help for fso.

%appfiles fso
    run_fso.bash bin/run_fso.bash

%appinstall fso
    chmod +x bin/run_fso.bash

%apprun fso
    echo "RUNNING FSO"
    run_fso.bash

%test
    /usr/local/bin/mpirun --allow-run-as-root /usr/bin/mpi_ring
