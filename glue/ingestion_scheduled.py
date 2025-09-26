import sys
import logging
from datetime import datetime

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
    ["JOB_NAME", "TARGET_BUCKET", "CONNECTION_NAME", "DB_TABLE"]
)

job_name = args["JOB_NAME"]
target_bucket = args["TARGET_BUCKET"]
connection_name = args["CONNECTION_NAME"]
db_table = args["DB_TABLE"]

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Target S3 bucket: {target_bucket}")
logger.info(f"MySQL connection: {connection_name}")
logger.info(f"Source table: {db_table}")


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

s3 = boto3.resource("s3")


# ============================================================
# Helper Functions
# ============================================================

def clear_s3_prefix(bucket: str, prefix: str) -> None:
    """
    Deletes all objects under the given S3 prefix.
    """
    prefix = prefix.rstrip("/")  # normalize
    bucket_obj = s3.Bucket(bucket)

    objects_to_delete = list(bucket_obj.objects.filter(Prefix=prefix))
    if not objects_to_delete:
        logger.warning(f"No objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Found {len(objects_to_delete)} objects to delete under s3://{bucket}/{prefix}")

    for i in range(0, len(objects_to_delete), 1000):  # API limit
        batch = objects_to_delete[i:i+1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})

    logger.info(f"Deleted existing content from s3://{bucket}/{prefix}")


def extract_from_mysql(table: str):
    """
    Extracts data from MySQL using Glue connection.
    """
    logger.info(f"Reading data from MySQL table: {table} ...")

    frame = glue_context.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": "true",
            "dbtable": table,
            "connectionName": connection_name,
        },
        transformation_ctx=f"mysqlNode_{table.replace('.', '_')}"
    )

    record_count = frame.count()
    logger.info(f"Extracted {record_count} records from {table}")
    return frame


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
    Writes the DynamicFrame to S3 in Parquet format, overwriting existing data (no partitioning).
    """
    output_prefix = f"{schema}_{table}/"
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


# ============================================================
# Main ETL Process
# ============================================================
try:
    schema_name, table_name = db_table.split(".")

    # 1. Extract
    mysql_frame = extract_from_mysql(db_table)

    # 2. Data Quality
    evaluate_data_quality(mysql_frame, db_table)

    # 3. Load
    write_to_s3(mysql_frame, schema_name, table_name)

    job.commit()
    logger.info(f"Glue job {job_name} completed successfully.")

except Exception as e:
    logger.error(f"Glue job {job_name} failed: {str(e)}", exc_info=True)
    job.commit()
    raise
