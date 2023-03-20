"""
DE: Lucas Contreras
Dag that ingest uniprot.xml data into neo4j database
config:

{"url":"https://rest.uniprot.org/uniprotkb/Q9Y261.xml"}

OR

{"url":"https://rest.uniprot.org/uniprotkb/A0A1B0GTW7.xml"}

"""
from datetime import datetime
from neo4j_etl.uniprot_etl import uniprot_etl

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook

with DAG(
    dag_id='uniprot_etl',
    schedule_interval=None,
    start_date=datetime(2023, 3, 17),
    catchup=False,
    tags=['neo4j'],
) as dag:
    # [START howto_operator_python]
    @task(task_id="load_into_neo4j")
    def load_into_neo4j(**kwargs):
        """Print the Airflow context and ds variable from the context."""
        url = kwargs['dag_run'].conf.get('url')
        if not url:
            raise ValueError('url cant be null. please add in the valid uniprot url config {"url":"uniprot_url"}')
        conn = BaseHook.get_connection('neo4j')
        elt = uniprot_etl(conn.host, conn.login, conn.password, url)
        elt.main()
        return 'Load succesful'

    run_this = load_into_neo4j()
    # [END howto_operator_python]