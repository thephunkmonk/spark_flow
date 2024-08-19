import sys
from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,

)

with DAG(
        'movie_spark',
        default_args={
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(seconds=3)
        },
        max_active_runs=1,
        max_active_tasks=3,
        description='movie',
        schedule="0 0 * * *",
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2015, 1, 5),
        catchup=True,
        tags=['api', 'movie', 'amt'],
) as dag:


    start = BashOperator(
        task_id="start",
        bash_command="echo 'start'"
    )

    def re_partition_func(ds_nodash):
        from sparkFlow.api import repartition
        # read_dir, write_dir, cnt = repartition(ds_nodash)
        print(repartition(ds_nodash))
        # print(f"""
        #     READ        --->    {read_dir}
        #     WRITE       --->    {write_dir}
        #     WRITE_CNT   --->    {cnt}
        # """)

    def join_df_func(ds_nodash):
        from sparkFlow.api import join_df
        rd, df = join_df(ds_nodash)
        print("read_dir -> " + rd)

        print(df)

    def agg_func(ds_nodash):
        from sparkFlow.api import agg
        r,w,d = agg(ds_nodash)    
        print(f"""
        READ    ->   {r},
        WRITE   ->   {w}
        """)
        print(d)

    re_partition = PythonVirtualenvOperator(
        task_id="re.partition",
        python_callable=re_partition_func,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.1.0/movie_flow"],
        system_site_packages=False,
        
    )
    
    join_df = PythonVirtualenvOperator(
        task_id="join.df",
        python_callable=join_df_func,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.1.0/movie_flow"],
        system_site_packages=False,
    )

    agg = PythonVirtualenvOperator(
        task_id="agg",
        python_callable=agg_func,
        requirements=["git+https://github.com/Jeonghoon2/spark_flow.git@d0.1.0/movie_flow"],
        system_site_packages=False,
    )


    end = BashOperator(
        task_id="end",
        bash_command="echo 'end'",
		trigger_rule="one_success"
    )

    start >> re_partition >> join_df >> agg >> end
