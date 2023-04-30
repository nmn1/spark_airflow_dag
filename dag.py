from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'run_spark_on_k8s',
    default_args=default_args,
    schedule_interval=None
)

spark_application = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {
        'name': 'my-spark-app',
        'namespace': 'spark-operator'
    },
    'spec': {
        'type': 'Python',
        'mode': 'cluster',
        'pythonVersion': '3',
        'image': 'gcr.io/spark-operator/spark-py:v3.1.1',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/examples/src/main/pyhton/pi.py',
        'sparkVersion': '3.1.1',
        'sparkConf': {
            'spark.executor.instances': '2',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1'
        }
    }
}

launch_spark_app = KubernetesPodOperator(
    task_id='launch_spark_app',
    name='spark-on-k8s',
    namespace='spark-operator',
    image='gcr.io/spark-operator/spark:v3.1.1',
    cmds=['/bin/bash', '-c'],
    arguments=[
        'echo starting spark application', 
        f'echo \'{json.dumps(spark_application)}\' > /spark_job/app/application.yaml',
        'kubectl apply -f /path/to/spark/application.yaml'
    ],
    volume_mounts=[{'name': 'spark-volume', 'mountPath': '/spark_job/app'}],
    volumes=[{'name': 'spark-volume', 'emptyDir': {}}],
    dag=dag
)
