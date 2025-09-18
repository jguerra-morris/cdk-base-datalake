import sys
import logging
import re

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
    ['JOB_NAME', 'TARGET_BUCKET', 'SOURCE_BUCKET', 'CONNECTION_NAME']
)

job_name = args['JOB_NAME']
source_bucket = args['SOURCE_BUCKET']
target_bucket = args['TARGET_BUCKET']
connection_name = args['CONNECTION_NAME']

logger.info(f"Starting Glue job: {job_name}")
logger.info(f"Source S3 bucket: {source_bucket}")
logger.info(f"Target S3 bucket: {target_bucket}")
logger.info(f"Redshift connection: {connection_name}")

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
# Clean column names for Redshift
# ----------------------------
def clean_column_name(name: str) -> str:
    if name.endswith('_$'):
        return (name[:-2] + '_USD').lower()
    if name.endswith('$'):
        return (name[:-1] + '_USD').lower()

    # Generic sanitization fallback
    new_name = re.sub(r'[^0-9a-zA-Z_]', '_', name)
    new_name = re.sub(r'__+', '_', new_name).strip('_')

    if re.match(r'^[0-9]', new_name):
        new_name = f"col_{new_name}"

    return new_name.lower()  # ✅ force lowercase for Redshift

def clean_dynamicframe_columns(dyf):
    df = dyf.toDF()
    new_cols = [clean_column_name(c) for c in df.columns]
    renamed_df = df.toDF(*new_cols)
    return DynamicFrame.fromDF(renamed_df, glueContext, "redshift_ready_node")

logger.info("Schema BEFORE sanitization:")
s3_source_node.printSchema()
logger.info(f"Columns BEFORE sanitization: {[f.name for f in s3_source_node.schema().fields]}")

logger.info("Sanitizing column names for Redshift compatibility ...")
redshift_ready_node = clean_dynamicframe_columns(s3_source_node)

logger.info("Schema AFTER sanitization:")
redshift_ready_node.printSchema()
logger.info(f"Columns AFTER sanitization: {[f.name for f in redshift_ready_node.schema().fields]}")

logger.info(f"Record count BEFORE sanitization: {s3_source_node.count()}")
logger.info(f"Record count AFTER sanitization: {redshift_ready_node.count()}")

# ----------------------------
# Load to Amazon S3 (✅ use sanitized data)
# ----------------------------
output_path = f"s3://{target_bucket}/SBO_CCP.POC_DIARIO/"
logger.info(f"Writing sanitized data to S3 path: {output_path}")
s3_target_node = glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready_node,   # ✅ use sanitized frame
    connection_type="s3", 
    format="glueparquet", 
    connection_options={
        "path": output_path, 
        "partitionKeys": []
    }, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="s3_target_node"
)
logger.info(f"Sanitized data successfully written to {output_path}")

# ----------------------------
# Load to Amazon Redshift
# ----------------------------
preactions_sql = """
DROP TABLE IF EXISTS public.POC_DIARIO;
CREATE TABLE IF NOT EXISTS public.POC_DIARIO (
    empresa VARCHAR,
    asiento INTEGER,
    linea INTEGER,
    fecha TIMESTAMP,
    acctcode VARCHAR,
    cc1 VARCHAR,
    cc2 VARCHAR,
    cc3 VARCHAR,
    cc4 VARCHAR,
    cc5 VARCHAR,
    debe_uf DECIMAL(18,2),
    haber_uf DECIMAL(18,2),
    debe_usd DECIMAL(18,2),
    haber_usd DECIMAL(18,2),
    debe_usd2 DECIMAL(18,2),
    haber_usd2 DECIMAL(18,2),
    debe_eur DECIMAL(18,2),
    haber_eur DECIMAL(18,2),
    ref1_cab VARCHAR,
    ref2_cab VARCHAR,
    ref3_cab VARCHAR,
    ref1_line VARCHAR,
    ref2_line VARCHAR,
    ref3_line VARCHAR,
    glosa VARCHAR,
    glosa_detalle VARCHAR
);
"""

# ✅ Strict validation: fail if mismatch
sanitized_columns = [f.name for f in redshift_ready_node.schema().fields]
create_table_cols = [c.split()[0] for c in re.findall(r'(\w+)\s+\w+', preactions_sql)]
missing_in_table = set(sanitized_columns) - set(create_table_cols)
if missing_in_table:
    raise Exception(f"Sanitized columns not present in CREATE TABLE: {sorted(missing_in_table)}")

logger.info("Writing sanitized data to Redshift table: public.POC_DIARIO")
redshift_node = glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready_node,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://{target_bucket}/aws-glue-assets/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.POC_DIARIO",
        "connectionName": connection_name,
        "preactions": preactions_sql
    },
    transformation_ctx="redshift_node"
)
logger.info("Data successfully written to Redshift table: public.POC_DIARIO")
