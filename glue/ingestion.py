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
# Extract from SAP HANA
# ----------------------------
logger.info("Reading data from SAP HANA table: SBO_CCP.POC_DIARIO ...")
sapNode = glueContext.create_dynamic_frame.from_options(
    connection_type="saphana",
    connection_options={
        "connectionName": connection_name,
        "dbtable": "SBO_CCP.POC_DIARIO"
    },
    transformation_ctx="sapNode"
)
record_count = sapNode.count()
logger.info(f"Extracted {record_count} records from SAP HANA.")

# ----------------------------
# Data Quality Evaluation
# ----------------------------
logger.info("Running Data Quality evaluation on extracted data ...")
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
logger.info("Data Quality evaluation completed and results published.")

# ----------------------------
# Load to Amazon S3
# ----------------------------
output_path = f"s3://{target_bucket}/SBO_CCP.POC_DIARIO"
logger.info(f"Writing data to S3 path: {output_path}")
s3Node = glueContext.write_dynamic_frame.from_options(
    frame=sapNode,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="s3Node"
)
logger.info(f"Data successfully written to {output_path}")

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
