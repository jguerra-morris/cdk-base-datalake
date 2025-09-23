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
    ["JOB_NAME", "TARGET_BUCKET", "CONNECTION_NAME", "SM_STAGE_A_ARN"]
)

job_name = args["JOB_NAME"]
target_bucket = args["TARGET_BUCKET"]
connection_name = args["CONNECTION_NAME"]
state_machine_arn = args["SM_STAGE_A_ARN"]

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Target S3 bucket: {target_bucket}")
logger.info(f"SAP HANA connection: {connection_name}")

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
    prefix = prefix.rstrip("/")  # normalize prefix
    bucket_obj = s3.Bucket(bucket)

    objects_to_delete = list(bucket_obj.objects.filter(Prefix=prefix))
    if not objects_to_delete:
        logger.warning(f"No objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Found {len(objects_to_delete)} objects to delete under s3://{bucket}/{prefix}")

    for i in range(0, len(objects_to_delete), 1000):  # S3 DeleteObjects API limit
        batch = objects_to_delete[i:i+1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})

    logger.info(f"Deleted existing content from s3://{bucket}/{prefix}")


def extract_from_sap(table: str):
    """
    Extracts data from SAP HANA using Glue connector.
    """
    logger.info(f"Reading data from SAP HANA table: {table} ...")

    sap_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="saphana",
        connection_options={"connectionName": connection_name, "dbtable": table},
        transformation_ctx=f"sapNode_{table.replace('.', '_')}"
    )

    record_count = sap_frame.count()
    logger.info(f"Extracted {record_count} records from {table}")
    return sap_frame


def evaluate_data_quality(frame, table: str):
    """
    Runs Data Quality checks on the extracted frame.
    """
    logger.info(f"Running Data Quality evaluation on table {table} ...")

    dq_results = EvaluateDataQuality().process_rows(
        frame=frame,
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

    logger.info(f"Data Quality evaluation completed for {table}")
    return dq_results


def write_to_s3(frame, schema: str, table: str) -> str:
    """
    Writes the DynamicFrame to S3 in Parquet format, after clearing existing partition.
    """
    output_prefix = f"{schema}_{table}/year={year}/month={month}/day={day}/"
    output_path = f"s3://{target_bucket}/{output_prefix}"

    clear_s3_prefix(target_bucket, output_prefix)

    logger.info(f"Writing data for {schema}.{table} to {output_path}")

    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": output_path, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx=f"s3Node_{schema}_{table}"
    )

    logger.info(f"Successfully written data for {schema}.{table} to {output_path}")
    return output_path


def trigger_step_function(payload: dict) -> None:
    """
    Triggers the configured Step Function with the given payload.
    """
    execution_name = f"{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    logger.info(f"Starting Step Function execution: {state_machine_arn} with payload {payload}")

    response = stepfunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(payload)
    )

    logger.info(f"Step Function started. Execution ARN: {response['executionArn']}")


# ============================================================
# Main ETL Loop
# ============================================================
for table in tables:
    schema_name, table_name = table.split(".")

    # 1. Extract
    sap_frame = extract_from_sap(table)

    # 2. Data Quality
    evaluate_data_quality(sap_frame, table)

    # 3. Load
    write_to_s3(sap_frame, schema_name, table_name)

# ============================================================
# Post-Processing: Trigger Step Function
# ============================================================
trigger_step_function(payload={"key": "SBO_CCP.POC_DIARIO"})

# ============================================================
# Commit Glue Job
# ============================================================
job.commit()
logger.info(f"Glue job {job_name} completed successfully.")
