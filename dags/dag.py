from datetime import timedelta, datetime, date
from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import yfinance as yf
import pandas as pd

ds = datetime.today().strftime('%Y-%m-%d')

default_args = {

    'owner': 'airflow',

    'depends_on_past': False,

    'start_date': datetime.today(),

    # 'email_on_failure': False,

    # 'email_on_retry': False,

    'retries': 2,

    'retry_delay': timedelta(minutes=5)

}

# python callable for t1: download APPL

# temp_dir = f"/tmp/"

def download_appl(): 

    start_date = date.today()

    end_date = start_date + timedelta(days=1)

    tsla_df = yf.download('AAPL', start=start_date, end=end_date, interval='1m')

    tsla_df.to_csv(f"/tmp/appl_{ds}.csv", header=False)

# python callable for t2: download TSLA

def download_tsla(): 

    start_date = date.today()

    end_date = start_date + timedelta(days=1)

    tsla_df = yf.download('TSLA', start=start_date, end=end_date, interval='1m')

    tsla_df.to_csv(f"/tmp/tsla_{datetime.today().strftime('%Y-%m-%d')}.csv", header=False)


with DAG(

    'marketvol',

    default_args=default_args,

    description='A simple DAG',

    schedule_interval='0 9 * * *'  # Cron expression for 6 PM on weekdays (Mon-Fri)

) as dag: 
    
    # create tmp directory for data download

    t0 = BashOperator(

        task_id='t0',

        bash_command='mkdir -p ~/data/' + ds,

        dag=dag
        
    )

    # download TSLA

    t1 = PythonOperator(

        task_id='t1',

        python_callable= download_appl,

        dag=dag
        
    )

    t2 = PythonOperator(

        task_id='t2',

        python_callable= download_tsla,

        dag=dag

    )

    # move appl data 
    # hdfs put command to load files into hdfs

    t3 = BashOperator(

        task_id='t3',

        bash_command= 'cp /tmp/appl_' + ds + '.csv  ~/data/' + ds,

        dag=dag

    )

    # move tsla data 

    t4 = BashOperator(

        task_id='t4',

        bash_command= 'cp /tmp/tsla_' + datetime.today().strftime('%Y-%m-%d') + '.csv  ~/data/' + datetime.today().strftime('%Y-%m-%d'),

        dag=dag

    )

    # custom qry
    # hdfs flag -head to show first few lines of file

    t5 =  BashOperator(

        task_id='t5',

        bash_command= 'head -n 5 ~/data/' + datetime.today().strftime('%Y-%m-%d') + '/appl_' + datetime.today().strftime('%Y-%m-%d') + '.csv && head -n 5 ~/data/' + datetime.today().strftime('%Y-%m-%d') + '/tsla_' + datetime.today().strftime('%Y-%m-%d') + '.csv',

        dag=dag

    )




##################################################################

## job dependencies

t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5
