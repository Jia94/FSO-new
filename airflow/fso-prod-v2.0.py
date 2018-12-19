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
    'fso-prod-00Z-v1.0',
    default_args=default_args,
    user_defined_macros={
        'npe': 12
    },
    schedule_interval='00 08,20 * * *')

check_obs_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
singularity exec -e -B FSO3.4:/FSO3.4 fso3.simg ./fso_check_obs.py
"""

t0 = BashOperator(
    task_id='check-obs',
    bash_command=check_obs_cmd,
    dag=dag)

check_icbc_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
singularity exec -e -B FSO3.4:/FSO3.4 fso3.simg ./fso_check_icbc.py
"""

t1 = BashOperator(
    task_id='check-icbc',
    bash_command=check_icbc_cmd,
    dag=dag)

check_ana_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
singularity exec -e -B FSO3.4:/FSO3.4 fso3.simg ./fso_check_ana.py
"""

t2 = BashOperator(
    task_id='check-ana',
    bash_command=check_ana_cmd,
    dag=dag)

da_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_da.py
"""

t3 = BashOperator(
    task_id='1-data-assimialtion',
    bash_command=da_cmd,
    dag=dag)

nl_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_nl.py
"""

t4 = BashOperator(
    task_id='2-1-nl-forecast',
    bash_command=nl_cmd,
    dag=dag)

force_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_forcing.py
"""

t5 = BashOperator(
    task_id='2-2-comp-forcing',
    bash_command=force_cmd,
    dag=dag)

adj_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_adj.py
"""

t6 = BashOperator(
    task_id='2-3-adj-backward',
    bash_command=adj_cmd,
    dag=dag)

err_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_err.py
"""

t7 = BashOperator(
    task_id='3-fcst-err',
    bash_command=err_cmd,
    dag=dag)

impact_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& SINGULARITYENV_CURR_DATE={{ts_nodash }} \
SINGULARITYENV_NPE={{ npe }} \
singularity exec -e -B FSO3.4:/FSO3.4 -B china_static:/gjx_static fso3.simg ./fso_impact.py
"""

t8 = BashOperator(
    task_id='4-fso-impact',
    bash_command=impact_cmd,
    dag=dag)

plot_cmd="""
ulimit -s unlimited  \
&& cd /home/dell/FSO \
&& CURR_DATE={{ts_nodash }} \
./fso_plot.py
"""

t9 = BashOperator(
    task_id='5-fso-plot',
    bash_command=plot_cmd,
    dag=dag)

t3.set_upstream([t2,t1,t0])
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)
t8.set_upstream(t7)
t9.set_upstream(t8)
