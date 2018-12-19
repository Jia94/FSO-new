"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,9,11),
    'email': ['xin.zhang@longrunweather.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'wrf-prod-00Z-v1.0', 
    default_args=default_args, 
    user_defined_macros={
        'npe': 12
    },
    schedule_interval='00 06,18 * * *')

check_gfs_command ="""
ulimit -s unlimited  \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static -B /data1/raw/gfs:/gfs fso3.simg ./scripts/wrf_check_gfs.py
"""
t0 = BashOperator(
    task_id='check-gfs',
    bash_command=check_gfs_command,
    dag=dag)

obsproc_command="""
ulimit -s unlimited  \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static -B /data1/input/little_r:/little_r fso3.simg ./scripts/wrf_obsproc.py
"""

t01 = BashOperator(
    task_id='obsproc',
    bash_command=obsproc_command,
    dag=dag)

wps_command="""
ulimit -s unlimited \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static -B /data1/raw/gfs:/gfs fso3.simg ./scripts/wrf_wps.py
"""

t1 = BashOperator(
    task_id='wps-prod',
    bash_command=wps_command,
    dag=dag)

ana_command="""
ulimit -s unlimited \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static fso3.simg ./scripts/wrf_real_ana.py
"""

t2 = BashOperator(
    task_id='real-ana-prod',
    bash_command=ana_command,
    dag=dag)

icbc_command="""
ulimit -s unlimited \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static fso3.simg ./scripts/wrf_real_icbc.py
"""

t3 = BashOperator(
    task_id='real-icbc-prod',
    bash_command=icbc_command,
    dag=dag)

wrf_command="""
ulimit -s unlimited \
&& cd /home/zwtd/FSO \
&& SINGULARITYENV_CURR_DATE={{ ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B china_FSO:/FSO3.4 -B china_working:/gjx_working -B china_static:/gjx_static fso3.simg ./scripts/wrf_prod.py
"""

t4 = BashOperator(
    task_id='wrf-prod',
    bash_command=wrf_command,
    dag=dag)

t01.set_upstream(t0)
t1.set_upstream(t0)
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
