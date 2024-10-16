'''
### ETL DAG ANS
ETL DAG file responsible to extract *"Sales of oil derivative fuels by UF and product"* 
and *"Sales of diesel by UF and type"*.
'''

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from datetime import datetime
from urllib import request

IN_PATH = 'files/in/'
OUT_PATH = 'files/out/'

with DAG(
    'ETL_ANS',
    description='ETL DAG ANS',
    schedule_interval=None,
    start_date=datetime(2022, 2, 1),
    catchup=False,
    tags=['excel', 'xls', 'pivot cache'],
) as dag:

    dag.doc_md = __doc__

    def extract(table):
        request.urlretrieve(f'http://192.168.1.6:5000/{table}', IN_PATH + table + '.csv')
        print('Extract done!')


    def transform(table):
        
        import numpy as np
        import locale
        locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')

        operationTypes = [
            'oil',
            'diesel'
        ]

        if table not in operationTypes:
            print("O valor informado para o parâmetro table (" + table + ") é inválido.")
            exit()

        try:

            df = pd.read_csv(IN_PATH + table + '.csv')

            df.columns = ['product', 'year', 'region', 'uf', 'unit', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'total']
            df.drop(columns=['region', 'total'], inplace=True)
            df = df.melt(id_vars=["product", "year", "uf", "unit"], var_name='month', value_name='volume')
            df.fillna(0, inplace=True)

            # Alguns valores não fora recuperados da Pivotcache
            df.drop(df[df.year == ''].index, inplace=True)
            df.drop(df[df.year == 0].index, inplace=True)

            df['volume'] = df['volume'].round(decimals = 3)
            df['year_month'] = df['month'] + '-' + df['year'].astype(int).astype(str)
            df['year_month'] = pd.to_datetime(df.year_month, format='%b-%Y', yearfirst=False)

            df.drop(columns=['year', 'month'], inplace=True)

            df.to_csv(OUT_PATH + table + '.csv', index=False)
            
            print('Transform Done!')

        except (Exception) as error:
            print(error)
            raise ValueError('Erro na etapa de transform.')


    def load(table):
        
        import psycopg2 as pg

        operationTypes = [
            'oil',
            'diesel'
        ]

        if table not in operationTypes:
            print("O valor informado para o parâmetro table (" + table + ") é inválido.")
            exit()

        try:
            conn = pg.connect(
                host="postgres",
                user="airflow",
                password="airflow"
            )
        except Exception as error:
            print(error)
            raise ValueError('Não foi possível conectar ao banco de dados.')

        cur = conn.cursor()

        # Criação do ambiente no Postgres
        commands = (
            '''
                CREATE SCHEMA IF NOT EXISTS ans
            ''',
            f'''
                CREATE TABLE IF NOT EXISTS ans.{table} (
                    id SERIAL PRIMARY KEY,
                    year_month date NOT NULL,
                    uf VARCHAR(100) NOT NULL,
                    product VARCHAR(100) NOT NULL,
                    unit VARCHAR(5) NOT NULL,
                    volume FLOAT NOT NULL,
                    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            ''',
            f'''
                TRUNCATE TABLE ans.{table}
            '''
        )

        try:

            for command in commands:
                cur.execute(command)

            conn.commit()

        except (Exception, pg.DatabaseError) as error:
            print(error)
            conn.rollback()
            raise ValueError('Ocorreu um erro ao configurar a base de dados.')

        try:

            data = pd.read_csv(OUT_PATH + table + '.csv')
            df = pd.DataFrame(data)

            for row in df.itertuples():

                cur.execute(
                    f'''
                        INSERT INTO ans.{table} (
                            year_month,
                            uf,
                            product,
                            unit,
                            volume
                        )
                        VALUES (%s, %s, %s, %s, %s)
                    ''',
                    (
                        row.year_month,
                        row.uf,
                        row.product,
                        row.unit,
                        row.volume
                    )
                )

            conn.commit()
            print("Load Done!")

        except (Exception, pg.DatabaseError) as error:
            print(error)
            conn.rollback()
            raise ValueError('Ocorreu um erro ao carregar os dados na base de destino. Volume: ' + str(row.volume))

        if conn is not None:
            conn.close()


    start_task = DummyOperator(
        task_id='start'
    )

    with TaskGroup("Extract") as extract_task:

        task_1 = PythonOperator(
            task_id='extract_oil',
            python_callable=extract,
            op_kwargs={'table': 'oil'},
        )

        task_2 = PythonOperator(
            task_id='extract_diesel',
            python_callable=extract,
            op_kwargs={'table': 'diesel'},
        )

    with TaskGroup("Transform") as transform_task:

        task_1 = PythonOperator(
            task_id='transform_oil',
            python_callable=transform,
            op_kwargs={'table': 'oil'},
        )

        task_2 = PythonOperator(
            task_id='transform_diesel',
            python_callable=transform,
            op_kwargs={'table': 'diesel'},
        )

    with TaskGroup("Load") as load_task:

        task_1 = PythonOperator(
            task_id='load_oil',
            python_callable=load,
            op_kwargs={'table': 'oil'},
        )

        task_2 = PythonOperator(
            task_id='load_diesel',
            python_callable=load,
            op_kwargs={'table': 'diesel'},
        )

        
    end_task = DummyOperator(
        task_id='end'
    )

    start_task >> extract_task >> transform_task >> load_task >> end_task