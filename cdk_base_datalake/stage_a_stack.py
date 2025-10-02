import sys
import logging
from datetime import datetime

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET"]
)

job_name = args["JOB_NAME"]
source_bucket = args["SOURCE_BUCKET"]
target_bucket = args["TARGET_BUCKET"]

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Source S3 bucket: {source_bucket}")
logger.info(f"Target S3 bucket: {target_bucket}")

# ============================================================
# Initialize Glue Context
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

s3 = boto3.resource("s3")

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
        logger.warning(f"No existing objects found under s3://{bucket}/{prefix}")
        return

    logger.info(f"Deleting {len(objects_to_delete)} objects from s3://{bucket}/{prefix}")

    for i in range(0, len(objects_to_delete), 1000):  # API batch limit
        batch = objects_to_delete[i:i+1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})

    logger.info(f"Cleared s3://{bucket}/{prefix}")


def extract_parquet_from_s3(path: str, ctx_name: str):
    """
    Reads Parquet data from the given S3 path into a DynamicFrame.
    """
    logger.info(f"Reading Parquet data from {path}")

    frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [path], "recurse": True},
        transformation_ctx=ctx_name
    )

    count = frame.count()
    logger.info(f"Extracted {count} records from {path}")
    return frame


def union_dynamicframes(frames: list, ctx_name: str) -> DynamicFrame:
    """
    Unions multiple DynamicFrames safely using Spark unionByName.
    """
    logger.info("Combining multiple datasets into one DynamicFrame...")

    dfs = [f.toDF() for f in frames]
    combined_df = dfs[0]

    for df in dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    combined_frame = DynamicFrame.fromDF(combined_df, glue_context, ctx_name)
    logger.info(f"Combined dataset contains {combined_frame.count()} records")
    return combined_frame


def write_to_s3(frame: DynamicFrame, prefix: str) -> str:
    """
    Writes a DynamicFrame to S3 in Parquet format (overwrite mode).
    """
    output_prefix = prefix.rstrip("/") + "/"
    output_path = f"s3://{target_bucket}/{output_prefix}"

    clear_s3_prefix(target_bucket, output_prefix)

    logger.info(f"Writing dataset to {output_path}")

    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        format="glueparquet",  # better compatibility with Athena
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
    arequipa_path = f"s3://{source_bucket}/delitos_arequipa/"
    junin_path = f"s3://{source_bucket}/rdsdb_delitos_junin/"

    delitos_arequipa = extract_parquet_from_s3(arequipa_path, "delitos_arequipa_dyf")
    delitos_junin = extract_parquet_from_s3(junin_path, "delitos_junin_dyf")

    # 2. Transform (Union)
    combined_delitos = union_dynamicframes(
        [delitos_arequipa, delitos_junin],
        ctx_name="combined_delitos_dyf"
    )

    # 3. Load
    write_to_s3(combined_delitos, prefix="delitos_all")

    job.commit()
    logger.info(f"Glue job {job_name} completed successfully.")

except Exception as e:
    logger.error(f"Glue job {job_name} failed: {str(e)}", exc_info=True)
    job.commit()
    raise
