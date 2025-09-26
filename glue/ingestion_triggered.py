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
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET"]
)

job_name = args["JOB_NAME"]
source_bucket = args["SOURCE_BUCKET"]
source_path = "s3://"+source_bucket
target_bucket = args["TARGET_BUCKET"]

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Source S3 path: {source_path}")
logger.info(f"Target S3 bucket: {target_bucket}")


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


def extract_from_s3(path: str):
    """
    Extracts JSON data from S3.
    """
    logger.info(f"Reading JSON data from S3 path: {path} ...")

    frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [path],
            "recurse": True
        },
        format_options={"multiLine": "false"},
        transformation_ctx="s3JsonSource"
    )

    record_count = frame.count()
    logger.info(f"Extracted {record_count} records from {path}")
    return frame


def evaluate_data_quality(frame, context: str):
    """
    Runs Data Quality checks on the extracted frame.
    """
    logger.info(f"Running Data Quality evaluation on dataset: {context} ...")

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

    logger.info(f"Data Quality evaluation completed for {context}")
    return dq_results


def write_to_s3(frame, prefix: str) -> str:
    """
    Writes the DynamicFrame to S3 in Parquet format, overwriting existing data (no partitioning).
    """
    output_prefix = f"{prefix.rstrip('/')}/"
    output_path = f"s3://{target_bucket}/{output_prefix}"

    clear_s3_prefix(target_bucket, output_prefix)

    logger.info(f"Writing data to {output_path}")

    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": output_path, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="s3ParquetSink"
    )

    logger.info(f"Successfully written data to {output_path}")
    return output_path


# ============================================================
# Main ETL Process
# ============================================================
try:
    # 1. Extract
    s3_frame = extract_from_s3(source_path)

    # 2. Data Quality
    evaluate_data_quality(s3_frame, source_path)

    # 3. Load
    write_to_s3(s3_frame, prefix="delitos_arequipa")

    job.commit()
    logger.info(f"Glue job {job_name} completed successfully.")

except Exception as e:
    logger.error(f"Glue job {job_name} failed: {str(e)}", exc_info=True)
    job.commit()
    raise
