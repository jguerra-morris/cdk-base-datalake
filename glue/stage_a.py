import sys
import logging
import json
from datetime import datetime, date

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# ============================================================
# Configure Logging
# ============================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ============================================================
# Parse Glue Job Arguments
# ============================================================
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'TARGET_BUCKET', 'SOURCE_BUCKET', 'SM_STAGE_B_ARN']
)

job_name = args['JOB_NAME']
source_bucket = args['SOURCE_BUCKET']
target_bucket = args['TARGET_BUCKET']
state_machine_arn = args['SM_STAGE_B_ARN']

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Source bucket: {source_bucket}, Target bucket: {target_bucket}")

# ============================================================
# Initialize Glue Context
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(job_name, args)

# ============================================================
# Constants & Configurations
# ============================================================
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""
logger.info("Default Data Quality ruleset defined.")

today = date.today()
year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")

tables = [
    "SBO_CCP.POC_DIARIO",
    "SBO_CCP.POC_PRESUPUESTO",
    "SBO_CCP.POC_ESTRUCTURA_EERR",
    "SBO_INMMMA_V1.POC_DIARIO",
    "SBO_INMMMA_V1.POC_PRESUPUESTO",
    "SBO_INMMMA_V1.POC_ESTRUCTURA_EERR"
]

s3 = boto3.resource("s3")
stepfunctions_client = boto3.client("stepfunctions")

# ============================================================
# Helper Functions
# ============================================================

def clear_s3_prefix(bucket: str, prefix: str) -> None:
    """
    Deletes all objects under the given S3 prefix.
    """
    prefix = prefix.rstrip("/")
    bucket_obj = s3.Bucket(bucket)
    objects_to_delete = list(bucket_obj.objects.filter(Prefix=prefix))

    if not objects_to_delete:
        logger.warning(f"No objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Deleting {len(objects_to_delete)} objects from s3://{bucket}/{prefix}")
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i:i + 1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})
    logger.info(f"Deleted objects under s3://{bucket}/{prefix}")


def extract_from_s3(schema: str, table: str) -> DynamicFrame:
    """
    Reads parquet files from S3 for the given table and date partition.
    """
    path = f"s3://{source_bucket}/{schema}_{table}/year={year}/month={month}/day={day}/"
    logger.info(f"Reading data from S3 path: {path}")

    frame = glue_context.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [path], "recurse": True},
        transformation_ctx=f"s3_source_{schema}_{table}"
    )

    logger.info(f"Extracted {frame.count()} records from {schema}.{table}")
    return frame


def evaluate_data_quality(frame: DynamicFrame, table: str) -> None:
    """
    Runs Data Quality evaluation on a DynamicFrame.
    """
    logger.info(f"Running Data Quality checks on {table}")
    EvaluateDataQuality().process_rows(
        frame=frame,
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
    logger.info(f"Data Quality checks completed for {table}")


def write_to_s3(frame: DynamicFrame, schema: str, table: str) -> str:
    """
    Writes DynamicFrame to S3 in parquet format with snappy compression.
    Clears the target prefix before writing.
    """
    prefix = f"{schema}_{table}/year={year}/month={month}/day={day}/"
    path = f"s3://{target_bucket}/{prefix}"

    clear_s3_prefix(target_bucket, prefix)

    logger.info(f"Writing data for {schema}.{table} to {path}")
    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": path, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx=f"s3_target_{schema}_{table}"
    )
    logger.info(f"Data successfully written for {schema}.{table}")
    return path


def trigger_step_function(payload: dict) -> None:
    """
    Starts a Step Function execution with the provided payload.
    """
    execution_name = f"{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"Triggering Step Function {state_machine_arn} with payload {payload}")

    response = stepfunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(payload)
    )
    logger.info(f"Step Function execution started: {response['executionArn']}")


# ============================================================
# Main ETL Loop
# ============================================================
for table in tables:
    schema_name, table_name = table.split(".")
    
    # 1. Extract
    df = extract_from_s3(schema_name, table_name)

    # 2. Data Quality
    evaluate_data_quality(df, table)

    # 3. Load
    write_to_s3(df, schema_name, table_name)

# ============================================================
# Post-Processing: Trigger Stage B Step Function
# ============================================================
trigger_step_function(payload={"key": "SBO_CCP.POC_DIARIO"})

# ============================================================
# Commit Glue Job
# ============================================================
job.commit()
logger.info(f"Glue job {job_name} completed successfully.")
