from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import time
from airflow.models import Variable


client = boto3.client('glue')
client1 = boto3.client('s3')

##########################################################
# CONSTANTS AND GLOBAL VARIABLES DEFINITION
##########################################################

# Connection Vars
PROJECT_PARAM = Variable.get("glue_config", deserialize_json=True)

#Get value in list
NumberOfWorkers = PROJECT_PARAM["NumberOfWorkers"]
WorkerType = PROJECT_PARAM["WorkerType"]

### glue job specific variables
glue_job_name1 = "my_glue_job1"
glue_job_name2 = "my_glue_job2"
glue_iam_role = "AWSGlueServiceRole"
region_name = "us-east-2"
email_recipient = "me@gmail.com"
##'retry_delay': timedelta(minutes=5)
script_parameter = ["s3://aws-airflow-demo-bucket/GlueScript/01cleaned.py", "s3://aws-airflow-demo-bucket/GlueScript/02joineddimcustomer.py", "s3://aws-airflow-demo-bucket/GlueScript/03goldview.py"]
job_parameter = ["glue_step1_cleaned_etl_", "glue_step2_transformjoined_etl_", "glue_step3_goldview_etl_"]

default_args = {
    'owner': 'me',
    'start_date': datetime(2021, 4, 1),   
    'retries': 2
}

##Athena query
ATHENA_SQL = ['msck repair table batch01_order_cleaned;', 'msck repair table batch02_customerordersummary;', 'msck repair table batch03_topcustomer;', 'msck repair table batch04_popularitem_uk;']
    
##########################################################
# DYNAMIC FUCNTION FOR GLUE 2.0
##########################################################
def glue2function(**kwargs):
    jobname= kwargs["jobname"]
    scriptlocation = kwargs["scriptlocation"]
    numberffworkers = kwargs["numberofworker"]
    auto = datetime.now()
    jobname1=jobname + str(auto)[:-7].strip()

    response = client.create_job(
    Name=jobname1,
    Role=glue_iam_role,
    ExecutionProperty={
        'MaxConcurrentRuns': 1
    },
    Command={
        'Name': 'glueetl',
        'ScriptLocation': scriptlocation,
        'PythonVersion': '3'
    },
    GlueVersion='2.0',
    NumberOfWorkers=numberffworkers,
    WorkerType='Standard',
    MaxRetries=2,
    Timeout=1440
    )

    time.sleep(10)

    job = client.start_job_run(JobName=jobname1)

    while True:
        status = client.get_job_run(JobName=jobname1, RunId=job['JobRunId'])
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':
            break
        time.sleep(10)


##########################################################
# S3 DELETE FUNCTION
##########################################################

def deletecleanedfunction():

    BUCKET = 'absilver'
    PREFIX = 'myer/silver/batch/01_order_cleaned/'
    try:
        paginator = client1.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=BUCKET,Prefix=PREFIX)
        for page in pages:
            for obj in page['Contents']:
                client1.delete_object(Bucket=BUCKET,Key=obj['Key'])
                #print(obj)
        
    except Exception:
        print('Delivery failure')

    time.sleep(40)


def deletecustomerfunction():

    BUCKET = 'absilver'
    PREFIX = 'myer/silver/batch/customerordersummary/'
    try:
        paginator = client1.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=BUCKET,Prefix=PREFIX)
        for page in pages:
            for obj in page['Contents']:
                client1.delete_object(Bucket=BUCKET,Key=obj['Key'])
                #print(obj)
        
    except Exception:
        print('Delivery failure')

    time.sleep(40)

def deletetopcustomerfunction():

    BUCKET = 'absilver'
    PREFIX = 'myer/silver/batch/topcustomer/'
    try:
        paginator = client1.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=BUCKET,Prefix=PREFIX)
        for page in pages:
            for obj in page['Contents']:
                client1.delete_object(Bucket=BUCKET,Key=obj['Key'])
                #print(obj)
        
    except Exception:
        print('Delivery failure')

    time.sleep(40)

 
def deletegoldfunction():

    BUCKET = 'absilver'
    PREFIX = 'myer/gold/popularitem_uk/'
    try:
        paginator = client1.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=BUCKET,Prefix=PREFIX)
        for page in pages:
            for obj in page['Contents']:
                client1.delete_object(Bucket=BUCKET,Key=obj['Key'])
                #print(obj)
        
    except Exception:
        print('Delivery failure')

    time.sleep(40)  


##########################################################
# DAG DEFINITIONS
##########################################################
dag = DAG(dag_id= 'maya_data_pipeline_v1',
          schedule_interval= None,
          catchup=False,
          default_args = default_args
          )


##########################################################
# DUMMY OPERATORS
##########################################################
# task order: first
start_flow = DummyOperator(
    task_id='start_flow',
    trigger_rule="dummy",
    dag=dag)

# task order: last
end_flow = DummyOperator(
    task_id='end_flow',
    dag=dag)



##########################################################
# S3 PYTHON DELETE OPERATOR
##########################################################

delete_cleanfunction = PythonOperator(
    #task_id='glue_job_step3',
    task_id='deletecleanedfunction',
    python_callable=deletecleanedfunction,
    dag=dag
    )


delete_customerfunction = PythonOperator(
    #task_id='glue_job_step3',
    task_id='deletecustomerfunction',
    python_callable=deletecustomerfunction,
    dag=dag
    )


delete_topcustomerfunction = PythonOperator(
    #task_id='glue_job_step3',
    task_id='deletetopcustomerfunction',
    python_callable=deletetopcustomerfunction,
    dag=dag
    )


delete_goldfunction = PythonOperator(
    #task_id='glue_job_step3',
    task_id='deletegold',
    python_callable=deletegoldfunction,
    dag=dag
    )

##########################################################
# GLUE 2.0 PYTHON OPERATORS
##########################################################
glue_step1_create_cleaned = PythonOperator(
    #task_id='glue_job_step3',
    task_id='glue_step1_create_cleaned',
    python_callable=glue2function,
    op_kwargs={'jobname': job_parameter[0], 'scriptlocation': script_parameter[0], 'numberofworker': NumberOfWorkers[2]},
    dag=dag,
    )


glue_step2_transformjoined_etl = PythonOperator(
    #task_id='glue_job_step3',
    task_id='glue_step2_transformjoined_etl',
    python_callable=glue2function,
    op_kwargs={'jobname': job_parameter[1], 'scriptlocation': script_parameter[1], 'numberofworker': NumberOfWorkers[3]},
    dag=dag,
    )

glue_step3_goldview_etl = PythonOperator(
    #task_id='glue_job_step3',
    task_id='glue_step3_goldview_etl',
    python_callable=glue2function,
    op_kwargs={'jobname': job_parameter[2], 'scriptlocation': script_parameter[2], 'numberofworker': NumberOfWorkers[4]},
    dag=dag,
    )


##########################################################
#ATHENA DYNAMIC OPERATORS
##########################################################
def create_dynamic_task(index_number, query):
    athena_update_cleaned = AWSAthenaOperator(
        task_id='Athena_Repair_' + str(index_number),
        query=query,
        aws_conn_id='aws_default',
        database='ordersilver',
        output_location='s3://query-results-nickdodo/s3://query-results-nickdodo/',
        dag=dag
    ) 

    return(athena_update_cleaned)


#########################################################
# ATHENA TASK FUNCTION
#########################################################
# Call athena operator
Tasks=[]
for index, x in enumerate(ATHENA_SQL, start=0):
    #task_dynamic_name = 'gm_step' + str(index+1)

    created_task = create_dynamic_task(index, x)
    Tasks.append(created_task)



#########################################################
# WORKFLOW DEFINITION
#########################################################

start_flow >> [delete_cleanfunction, delete_customerfunction,delete_topcustomerfunction, delete_goldfunction] \
>> glue_step1_create_cleaned >> glue_step2_transformjoined_etl >> glue_step3_goldview_etl >> Tasks[0] >> Tasks[1] >> Tasks[2] \
>> Tasks[3] >> end_flow

  