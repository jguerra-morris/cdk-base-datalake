import sys
import logging
import pandas as pd
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import functions as F
import json
import boto3

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
stepfunctions_client = boto3.client("stepfunctions")

# ============================================================
# Parse Job Parameters
# ============================================================
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET", "SM_STAGE_B_ARN", "CATALOG_NAME", "TABLE_NAME"]
)
job_name = args["JOB_NAME"]
source_bucket = args["SOURCE_BUCKET"]
target_bucket = args["TARGET_BUCKET"]
state_machine_arn = args["SM_STAGE_B_ARN"]
catalog_name = args["CATALOG_NAME"]

logger.info(f"Starting AWS Glue Job: {job_name}")

# ============================================================
# Initialize Glue Context and Spark Session
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(job_name, args)

# ============================================================
# Constants & Configuration
# ============================================================
SOURCE_PATH = "s3://"+source_bucket+"/tabla_modelo_segmentacion.xlsx"
TARGET_PATH = "s3://"+target_bucket
CATALOG_TABLE = args["TABLE_NAME"]

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
logger.info("Spark configuration and default data quality ruleset initialized.")

# ============================================================
# Helper Functions
# ============================================================

def read_excel_from_s3(path: str) -> DynamicFrame:
    """
    Reads an Excel file from S3 into a Glue DynamicFrame.
    """
    logger.info(f"Reading Excel file from S3: {path}")
    df_pandas = pd.read_excel(path)
    schema = StructType([StructField(c, StringType(), True) for c in df_pandas.columns])
    df_spark = spark.createDataFrame(df_pandas, schema=schema)
    dynamic_frame = DynamicFrame.fromDF(df_spark, glue_context, "input_data")
    record_count = df_spark.count()
    logger.info(f"Successfully read {record_count} records from {path}")
    return dynamic_frame

def clean_sensitive_data(input_frame: DynamicFrame, ctx: GlueContext) -> DynamicFrame:
    """
    Masks the first four characters of the AFIL_NRUT column using PySpark directly.
    """
    logger.info("Starting data cleaning transformation (masking AFIL_NRUT)...")

    # Convert Glue DynamicFrame to Spark DataFrame
    df = input_frame.toDF()

    # Verify the column exists before applying transformation
    if "AFIL_NRUT" not in df.columns:
        logger.warning("Column 'AFIL_NRUT' not found. Skipping masking step.")
        return input_frame

    # Mask the first 4 characters of the AFIL_NRUT column
    df_cleaned = df.withColumn(
        "AFIL_NRUT",
        F.concat(F.lit("****"), F.expr("substring(AFIL_NRUT, 5, length(AFIL_NRUT))"))
    )

    logger.info("Sensitive data masking completed using PySpark.")

    # Convert back to DynamicFrame for Glue compatibility
    return DynamicFrame.fromDF(df_cleaned, ctx, "cleaned_data")

def evaluate_data_quality(frame: DynamicFrame) -> None:
    """
    Evaluates data quality rules on the provided DynamicFrame.
    """
    logger.info("Running data quality checks...")
    dq_results = EvaluateDataQuality().process_rows(
        frame=frame,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "DQ_Context",
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        },
    )
    logger.info("Data quality evaluation completed.")
    return dq_results


def write_to_s3_parquet(frame: DynamicFrame) -> None:
    """
    Writes the processed DynamicFrame to S3 in Parquet format
    and updates the Glue Catalog table.
    """
    logger.info(f"Writing processed data to S3 path: {TARGET_PATH}")
    sink = glue_context.getSink(
        path=TARGET_PATH,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="s3_output_sink"
    )
    sink.setCatalogInfo(
        catalogDatabase=catalog_name,
        catalogTableName=CATALOG_TABLE
    )
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(frame)
    logger.info(f"Data successfully written to {TARGET_PATH} and catalog updated.")



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
# Main ETL Flow
# ============================================================
try:
    # Step 1: Extract
    input_data = read_excel_from_s3(SOURCE_PATH)

    # Step 2: Transform
    cleaned_data = clean_sensitive_data(input_data, glue_context)

    # Step 3: Data Quality
    evaluate_data_quality(cleaned_data)

    # Step 4: Load
    write_to_s3_parquet(cleaned_data)

    # Ste 5: Post Processing
    trigger_step_function(payload={"key": "SBO_CCP.POC_DIARIO"})
    logger.info("Post-processing steps completed.")


    logger.info(f"Glue job {job_name} completed successfully.")

except Exception as e:
    logger.error(f"Error occurred in Glue job {job_name}: {str(e)}", exc_info=True)
    raise

finally:
    job.commit()
    logger.info("Glue job committed and terminated gracefully.")
