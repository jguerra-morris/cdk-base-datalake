import sys
import logging
import boto3
import json
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'TARGET_BUCKET', 'SOURCE_BUCKET', 'SM_STAGE_B_ARN']
)

job_name = args['JOB_NAME']
source_bucket = args['SOURCE_BUCKET']
target_bucket = args['TARGET_BUCKET']
state_machine_arn = args['SM_STAGE_B_ARN']

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Source S3 bucket: {source_bucket}")
logger.info(f"Target S3 bucket: {target_bucket}")

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
# Extract from Amazon S3
# ----------------------------
logger.info(f"Reading data from S3 path: s3://{source_bucket}/SBO_CCP.POC_DIARIO/ ...")
s3_source_node = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={
        "paths": [f"s3://{source_bucket}/SBO_CCP.POC_DIARIO/"], 
        "recurse": True
    }, 
    transformation_ctx="s3_source_node"
)
record_count = s3_source_node.count()
logger.info(f"Extracted {record_count} records from source S3.")

# ----------------------------
# Data Quality Evaluation
# ----------------------------
logger.info("Running Data Quality evaluation on extracted data ...")
dq_results = EvaluateDataQuality().process_rows(
    frame=s3_source_node, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "dq_node",
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
output_path = f"s3://{target_bucket}/SBO_CCP.POC_DIARIO/"
logger.info(f"Writing data to S3 path: {output_path}")
s3_target_node = glueContext.write_dynamic_frame.from_options(
    frame=s3_source_node, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={
        "path": output_path, 
        "partitionKeys": []
    }, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="s3_target_node"
)
logger.info(f"Data successfully written to {output_path}")


# ----------------------------
# Trigger Stage B Step Function
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

