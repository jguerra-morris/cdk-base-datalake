import sys
import logging
import re
from datetime import datetime, date
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import expr, col, date_format, coalesce, lit

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

# ============================================================
# Initialize Glue Context
# ============================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# ============================================================
# Global Constants
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


def extract_and_evaluate(table_name: str, alias: str) -> DynamicFrame:
    """
    Extracts data from S3 path and runs Data Quality evaluation.
    """
    path = f"s3://{source_bucket}/{table_name}/year={year}/month={month}/day={day}/"
    logger.info(f"Reading data from S3 path: {path} ...")

    dyf = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [path], "recurse": True},
        transformation_ctx=f"s3_source_node_{alias}"
    )

    record_count = dyf.count()
    logger.info(f"Extracted {record_count} records from {table_name}")

    logger.info("Running Data Quality evaluation...")
    EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": f"dq_node_{alias}",
            "enableDataQualityResultsPublishing": True
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        }
    )
    logger.info(f"Data Quality evaluation completed for {table_name}")
    return dyf


def unpivot_dynamicframe(dyf: DynamicFrame, new_name="unpivoted") -> DynamicFrame:
    """
    Converts monthly columns (V_ENE ... V_DIC) to 'mes' and 'monto'.
    Keeps all other columns intact.
    """
    month_cols = ["V_ENE","V_FEB","V_MAR","V_ABR","V_MAY","V_JUN",
                  "V_JUL","V_AGO","V_SEP","V_OCT","V_NOV","V_DIC"]

    df = dyf.toDF()
    other_cols = [c for c in df.columns if c not in month_cols]

    expr_str = "stack({0}, {1}) as (mes, monto)".format(
        len(month_cols),
        ", ".join([f"'{i+1}', `{col}`" for i, col in enumerate(month_cols)])
    )

    df_unpivot = df.selectExpr(*other_cols, expr_str)
    df_unpivot = df_unpivot.withColumn("mes", col("mes").cast("int"))

    return DynamicFrame.fromDF(df_unpivot, glueContext, new_name)


def unpivot_monedas(dyf: DynamicFrame, new_name="unpivoted") -> DynamicFrame:
    """
    Converts columns like Debe_$, Haber_$, Debe_UF, Haber_UF, etc.
    into columns: Moneda, Debe, Haber.
    Keeps all other columns intact.
    """
    df = dyf.toDF()
    monedas = ["$", "UF", "USD", "EUR"]

    expr_parts = []
    for moneda in monedas:
        col_debe = f"Debe_{moneda}"
        col_haber = f"Haber_{moneda}"
        if col_debe in df.columns and col_haber in df.columns:
            expr_parts.append(f"'{moneda}', cast(`{col_debe}` as double), cast(`{col_haber}` as double)")

    if not expr_parts:
        raise ValueError("No currency columns found in DataFrame.")

    expr_str = f"stack({len(expr_parts)}, {', '.join(expr_parts)}) as (Moneda, Debe, Haber)"

    df_unpivoted = df.select(
        *[col(c) for c in df.columns if not c.startswith("Debe_") and not c.startswith("Haber_")],
        expr(expr_str)
    )

    return DynamicFrame.fromDF(df_unpivoted, glueContext, new_name)

# ============================================================
# Extract and Transform Tables
# ============================================================

# --- SBO_CCP ---
sbo_poc_presupuesto = extract_and_evaluate("SBO_CCP_POC_PRESUPUESTO", "sbo_poc_presupuesto")
sbo_poc_diario = extract_and_evaluate("SBO_CCP_POC_DIARIO", "sbo_poc_diario")
sbo_poc_estructura = extract_and_evaluate("SBO_CCP_POC_ESTRUCTURA_EERR", "sbo_poc_estructura")

# --- SBO_INMMMA_V1 ---
inmmma_poc_presupuesto = extract_and_evaluate("SBO_INMMMA_V1_POC_PRESUPUESTO", "inmmma_poc_presupuesto")
inmmma_poc_diario = extract_and_evaluate("SBO_INMMMA_V1_POC_DIARIO", "inmmma_poc_diario")
inmmma_poc_estructura = extract_and_evaluate("SBO_INMMMA_V1_POC_ESTRUCTURA_EERR", "inmmma_poc_estructura")

# --- Apply Unpivot ---
# SBO_CCP
sbo_poc_diario_unpivot = unpivot_monedas(sbo_poc_diario, "sbo_poc_diario_unpivot")
sbo_poc_presupuesto_unpivot = unpivot_dynamicframe(sbo_poc_presupuesto, "sbo_poc_presupuesto_unpivot")

# SBO_INMMMA_V1
inmmma_poc_diario_unpivot = unpivot_monedas(inmmma_poc_diario, "inmmma_poc_diario_unpivot")
inmmma_poc_presupuesto_unpivot = unpivot_dynamicframe(inmmma_poc_presupuesto, "inmmma_poc_presupuesto_unpivot")

# ============================================================
# Join Diario - Presupuesto (SBO_CCP)
# ============================================================
india_df = sbo_poc_diario_unpivot.toDF()
inpre_df = sbo_poc_presupuesto_unpivot.toDF()

india_df = india_df.withColumn("Periodo", date_format(col("fecha"), "yyyy").cast("int")) \
                   .withColumn("mes", date_format(col("fecha"), "MM").cast("int"))

inpre_df = inpre_df.select([col(c).alias(f"pre_{c}") for c in inpre_df.columns])

join_condition = (
    (india_df["Acctcode"] == inpre_df["pre_acctcode"]) &
    (india_df["Periodo"] == inpre_df["pre_Periodo"]) &
    (india_df["mes"] == inpre_df["pre_mes"]) &
    (india_df["Moneda"] == inpre_df["pre_Moneda"]) &
    (coalesce(india_df["cc1"], lit("0")) == coalesce(inpre_df["pre_cc1"], lit("0"))) &
    (coalesce(india_df["cc2"], lit("0")) == coalesce(inpre_df["pre_cc2"], lit("0"))) &
    (coalesce(india_df["cc3"], lit("0")) == coalesce(inpre_df["pre_cc3"], lit("0"))) &
    (coalesce(india_df["cc4"], lit("0")) == coalesce(inpre_df["pre_cc4"], lit("0"))) &
    (coalesce(india_df["cc5"], lit("0")) == coalesce(inpre_df["pre_cc5"], lit("0")))
)

joined_sbo = india_df.join(inpre_df, join_condition, "left")
joined_sbo_dyf = DynamicFrame.fromDF(joined_sbo, glueContext, "joined_sbo_dyf")

# ============================================================
# Join Diario - Presupuesto (SBO_INMMMA_V1)
# ============================================================
india_df = inmmma_poc_diario_unpivot.toDF()
inpre_df = inmmma_poc_presupuesto_unpivot.toDF()

india_df = india_df.withColumn("Periodo", date_format(col("fecha"), "yyyy").cast("int")) \
                   .withColumn("mes", date_format(col("fecha"), "MM").cast("int"))

inpre_df = inpre_df.select([col(c).alias(f"pre_{c}") for c in inpre_df.columns])

join_condition = (
    (india_df["Acctcode"] == inpre_df["pre_acctcode"]) &
    (india_df["Periodo"] == inpre_df["pre_Periodo"]) &
    (india_df["mes"] == inpre_df["pre_mes"]) &
    (india_df["Moneda"] == inpre_df["pre_Moneda"]) &
    (coalesce(india_df["cc1"], lit("0")) == coalesce(inpre_df["pre_cc1"], lit("0"))) &
    (coalesce(india_df["cc2"], lit("0")) == coalesce(inpre_df["pre_cc2"], lit("0"))) &
    (coalesce(india_df["cc3"], lit("0")) == coalesce(inpre_df["pre_cc3"], lit("0"))) &
    (coalesce(india_df["cc4"], lit("0")) == coalesce(inpre_df["pre_cc4"], lit("0"))) &
    (coalesce(india_df["cc5"], lit("0")) == coalesce(inpre_df["pre_cc5"], lit("0")))
)

joined_inmmma = india_df.join(inpre_df, join_condition, "left").dropDuplicates()
joined_inmmma_dyf = DynamicFrame.fromDF(joined_inmmma, glueContext, "joined_inmmma_dyf")

# ============================================================
# Join Estructura Tables
# ============================================================

# --- SBO_CCP Estructura ---
sbo_estructura_df = sbo_poc_estructura.toDF().select([col(c).alias(f"est_{c}") for c in sbo_poc_estructura.toDF().columns])
joined_sbo_with_est = joined_sbo.join(
    sbo_estructura_df,
    joined_sbo["Acctcode"] == sbo_estructura_df["est_Acctcode"],
    "left"
)

# --- SBO_INMMMA_V1 Estructura ---
inmmma_estructura_df = inmmma_poc_estructura.toDF().select([col(c).alias(f"est_{c}") for c in inmmma_poc_estructura.toDF().columns])
joined_inmmma_with_est = joined_inmmma.join(
    inmmma_estructura_df,
    joined_inmmma["Acctcode"] == inmmma_estructura_df["est_Acctcode"],
    "left"
)

# ============================================================
# Union Both Sources
# ============================================================
union_df = joined_sbo_with_est.unionByName(joined_inmmma_with_est)

# ============================================================
# Type Casting
# ============================================================
union_df = union_df.withColumn("fecha", col("fecha").cast("timestamp")) \
                   .withColumn("debe", col("debe").cast("decimal(18,2)")) \
                   .withColumn("haber", col("haber").cast("decimal(18,2)")) \
                   .withColumn("pre_monto", col("pre_monto").cast("decimal(18,2)"))

joined_union_dyf = DynamicFrame.fromDF(union_df, glueContext, "joined_union_dyf")

# ============================================================
# Clean Column Names for Redshift
# ============================================================

def clean_column_name(name: str) -> str:
    """Sanitize column names for Redshift compatibility."""
    if name.endswith('_$'):
        return (name[:-2] + '_USD').lower()
    if name.endswith('$'):
        return (name[:-1] + '_USD').lower()
    # Generic sanitization
    new_name = re.sub(r'[^0-9a-zA-Z_]', '_', name)
    new_name = re.sub(r'__+', '_', new_name).strip('_')
    if re.match(r'^[0-9]', new_name):
        new_name = f"col_{new_name}"
    return new_name.lower()

def clean_dynamicframe_columns(dyf: DynamicFrame) -> DynamicFrame:
    df = dyf.toDF()
    new_cols = [clean_column_name(c) for c in df.columns]
    renamed_df = df.toDF(*new_cols)
    return DynamicFrame.fromDF(renamed_df, glueContext, "redshift_ready_dyf")

logger.info("Sanitizing column names for Redshift compatibility...")
redshift_ready_node = clean_dynamicframe_columns(joined_union_dyf)


logger.info("Schema AFTER sanitization:")
redshift_ready_node.printSchema()
logger.info(f"Columns AFTER sanitization: {[f.name for f in redshift_ready_node.schema().fields]}")

# ============================================================
# Write to Amazon S3 (Sanitized)
# ============================================================
output_prefix = "SBO_CCP_POC_DIARIO_PRESUPUESTO"
output_path = f"s3://{target_bucket}/{output_prefix}/"

clear_s3_prefix(target_bucket, output_prefix)

logger.info(f"Writing sanitized data to S3 path: {output_path}")
glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready_node,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": output_path, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="s3_target_node"
)
logger.info(f"Sanitized data successfully written to {output_path}")

# ============================================================
# Write to Amazon Redshift
# ============================================================
preactions_sql = """
CREATE TABLE IF NOT EXISTS public.poc_join (
    empresa         VARCHAR,
    asiento         INT,
    linea           INT,
    fecha           TIMESTAMP,
    acctcode        VARCHAR,
    cc1             VARCHAR,
    cc2             VARCHAR,
    cc3             VARCHAR,
    cc4             VARCHAR,
    cc5             VARCHAR,
    n_cc1           VARCHAR,
    n_cc2           VARCHAR,
    n_cc3           VARCHAR,
    n_cc4           VARCHAR,
    n_cc5           VARCHAR,
    ref1_cab        VARCHAR,
    ref2_cab        VARCHAR,
    ref3_cab        VARCHAR,
    ref1_line       VARCHAR,
    ref2_line       VARCHAR,
    ref3_line       VARCHAR,
    glosa           VARCHAR(1000),
    glosa_detalle   VARCHAR(1000),
    moneda          VARCHAR,
    debe            DECIMAL(18,2),
    haber           DECIMAL(18,2),
    periodo         INT,
    mes             INT,
    pre_empresa     VARCHAR,
    pre_periodo     INT,
    pre_moneda      VARCHAR,
    pre_acctcode    VARCHAR,
    pre_cc1         VARCHAR,
    pre_cc2         VARCHAR,
    pre_cc3         VARCHAR,
    pre_cc4         VARCHAR,
    pre_cc5         VARCHAR,
    pre_n_cc1       VARCHAR,
    pre_n_cc2       VARCHAR,
    pre_n_cc3       VARCHAR,
    pre_n_cc4       VARCHAR,
    pre_n_cc5       VARCHAR,
    pre_mes         INT,
    pre_monto       DECIMAL(18,2),
    est_empresa     VARCHAR,
    est_visorden1   INT,
    est_visorden2   INT,
    est_visorden3   INT,
    est_visorden4   INT,
    est_grupo       VARCHAR,
    est_subgrupo1   VARCHAR,
    est_subgrupo2   VARCHAR,
    est_subgrupo3   VARCHAR,
    est_acctcode    VARCHAR,
    est_acctname    VARCHAR
);
TRUNCATE TABLE public.poc_join;
"""

logger.info("Writing sanitized data to Redshift table: public.poc_join")
glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready_node,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://{target_bucket}/aws-glue-assets/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.poc_join",
        "connectionName": connection_name,
        "preactions": preactions_sql,
        "copyoptions": "FORMAT AS PARQUET"
    },
    transformation_ctx="redshift_node"
)
logger.info("Data successfully written to Redshift table: public.poc_join")

# ============================================================
# Commit Glue Job
# ============================================================
job.commit()
logger.info(f"Glue job {job_name} completed successfully.")
