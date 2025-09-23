import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import boto3
import json
from datetime import datetime
from datetime import date

# ----------------------------
# Configure logging
# ----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ----------------------------
# Parse Glue job arguments
# ----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TARGET_BUCKET', 'CONNECTION_NAME', 'SM_STAGE_A_ARN'])
job_name = args['JOB_NAME']
target_bucket = args['TARGET_BUCKET']
connection_name = args['CONNECTION_NAME']
state_machine_arn = args['SM_STAGE_A_ARN']

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Target S3 bucket: {target_bucket}")
logger.info(f"SAP HANA connection: {connection_name}")

# ----------------------------
# Initialize contexts
# ----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# ----------------------------
# Define Data Quality Rules
# ----------------------------
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""
logger.info("Default Data Quality ruleset defined.")


# ----------------------------
# Helper function to clear S3 path
# ----------------------------
s3 = boto3.resource("s3")

def clear_s3_prefix(bucket, prefix):
    # Ensure no trailing slash, S3 keys don't need it
    prefix = prefix.rstrip("/")
    bucket_obj = s3.Bucket(bucket)

    objects_to_delete = list(bucket_obj.objects.filter(Prefix=prefix))
    if not objects_to_delete:
        logger.warning(f"No objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Found {len(objects_to_delete)} objects to delete under s3://{bucket}/{prefix}")

    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i:i+1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})

    logger.info(f"Deleted existing content from s3://{bucket}/{prefix}")

#Cambio jamado 2025-09-19

# Fecha actual para particiones
today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# Tablas a procesar
tables = [
    "SBO_CCP.POC_DIARIO",
    "SBO_CCP.POC_PRESUPUESTO",
    "SBO_CCP.POC_ESTRUCTURA_EERR",
    "SBO_INMMMA_V1.POC_DIARIO",
    "SBO_INMMMA_V1.POC_PRESUPUESTO",
    "SBO_INMMMA_V1.POC_ESTRUCTURA_EERR"
]


# ----------------------------
# Loop para procesar todas las tablas
# ----------------------------
for table in tables:
    logger.info(f"Reading data from SAP HANA table: {table} ...")
    # ----------------------------
    # Extract from SAP HANA
    # ----------------------------    
    sapNode = glueContext.create_dynamic_frame.from_options(
        connection_type="saphana",
        connection_options={
            "connectionName": connection_name,
            "dbtable": table
        },
        transformation_ctx=f"sapNode_{table.replace('.', '_')}"
    )

    record_count = sapNode.count()
    logger.info(f"Extracted {record_count} records from {table}.")

    # ----------------------------
    # Data Quality Evaluation
    # ----------------------------
    logger.info(f"Running Data Quality evaluation on table {table} ...")
    dq_results = EvaluateDataQuality().process_rows(
        frame=sapNode,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "dqNode",
            "enableDataQualityResultsPublishing": True
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        }
    )
    logger.info(f"Data Quality evaluation completed for {table}.")


    # ----------------------------
    # Load to Amazon S3 with overwrite
    # ----------------------------
    table_name = table.split(".")[-1]  # ejemplo: POC_DIARIO
    schema_name = table.split(".")[0]  # ejemplo: SBO_CCP

    output_prefix = f"{schema_name}_{table_name}/year={year}/month={month}/day={day}/"
    output_path = f"s3://{target_bucket}/{output_prefix}"

    # Delete existing content before writing
    clear_s3_prefix(target_bucket, output_prefix)

    logger.info(f"Writing data from {table} to S3 path: {output_path}")

    glueContext.write_dynamic_frame.from_options(
        frame=sapNode,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": output_path,
            "partitionKeys": []
        },
        format_options={"compression": "snappy"},
        transformation_ctx=f"s3Node_{table.replace('.', '_')}"
    )

    logger.info(f"Data successfully written for {table} -> {output_path}")


#Cambio jamado 2025-09-19

# ----------------------------
# Trigger Dtage A Step Function
# ----------------------------

stepfunctions_client = boto3.client('stepfunctions')

payload = {
    "key": "SBO_CCP.POC_DIARIO"
}

execution_name = f"{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

logger.info(f"Starting Step Function execution: {state_machine_arn} with payload {payload}")
response = stepfunctions_client.start_execution(
    stateMachineArn=state_machine_arn,
    name=execution_name,
    input=json.dumps(payload)
)
logger.info(f"Step Function started. Execution ARN: {response['executionArn']}")



# ----------------------------
# Commit job
# ----------------------------
job.commit()
logger.info(f"Glue job {job_name} completed successfully.")
