from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client.models import V1VolumeMount, V1Volume
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
        'namespace': 'default'
    },
    'spec': {
        'type': 'Python',
        'mode': 'cluster',
        'pythonVersion': '3',
        'image': 'gcr.io/spark-operator/spark-py:v3.1.1',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/examples/src/main/pyhton/pi.py',
        'sparkVersion': '3.1.1',
        'restartPolicy': {
           'type': 'OnFailure',
           'onFailureRetries': '3',
           'onFailureRetryInterval': '10',
           'onSubmissionFailureRetries': '5',
           'onSubmissionFailureRetryInterval': '20',
        },   
        'dynamicAllocation': {
           'enabled': 'true',
           'initialExecutors': '2',
           'minExecutors': '2',
           'maxExecutors': '20',
        },   
        'driver': {
           'cores': '1',
           'coreLimit': "1200m",
           'memory': "512m",
           'labels': {
              'version': '3.1.1'
           },  
           'serviceAccount': 'my-release-spark'
        },
        'executor': {
           'cores': '1',
           'instances': '1',
           'memory': "512m",
           'labels': {
              'version': '3.1.1'
            }, 
           'serviceAccount': 'my-release-spark'
        }
        
    }
}

volume_mounts = [V1VolumeMount(name='spark-volume', mount_path='/spark_job/app')]

volumes = [
    V1Volume(name='spark-volume', empty_dir={}, config_map=None, host_path=None, persistent_volume_claim=None,
             secret=None, downward_api=None, projected=None)
]


launch_spark_app = KubernetesPodOperator(
    task_id='launch_spark_app',
    name='spark-on-k8s',
    namespace='default',
    image='bitnami/kubectl:latest',
    cmds=['/bin/bash', '-c'],
    arguments=[
        'echo starting spark application', 
        f'echo \'{json.dumps(spark_application)}\' > /spark_job/app/application.yaml',
        'cat /spark_job/app/application.yaml',
        'kubectl apply -f /spark_job/app/application.yaml'
    ],
    volume_mounts=volume_mounts,
    volumes=volumes,
    get_logs=True,
    dag=dag,
    termination_grace_period_seconds=60
)
