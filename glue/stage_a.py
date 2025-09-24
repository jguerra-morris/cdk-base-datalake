import sys
import logging
import re
from datetime import date, datetime

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, coalesce, lit, expr, date_format

# ============================================================
# Logging Configuration
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
logger.info(f"Source bucket: {source_bucket}, Target bucket: {target_bucket}, Redshift connection: {connection_name}")

# ============================================================
# Initialize Glue Context
# ============================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# ============================================================
# Constants & Configurations
# ============================================================
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""

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
    """Deletes all objects under a given S3 prefix."""
    prefix = prefix.rstrip("/")
    bucket_obj = s3.Bucket(bucket)
    objs_to_delete = list(bucket_obj.objects.filter(Prefix=prefix))
    if not objs_to_delete:
        logger.warning(f"No objects found under s3://{bucket}/{prefix}")
        return
    logger.info(f"Deleting {len(objs_to_delete)} objects from s3://{bucket}/{prefix}")
    for i in range(0, len(objs_to_delete), 1000):
        batch = objs_to_delete[i:i + 1000]
        bucket_obj.delete_objects(Delete={"Objects": [{"Key": obj.key} for obj in batch]})
    logger.info(f"Deleted objects under s3://{bucket}/{prefix}")

def extract_from_s3(table_name: str, alias: str) -> DynamicFrame:
    """Extracts data from S3 and evaluates data quality."""
    path = f"s3://{source_bucket}/{table_name}/year={year}/month={month}/day={day}/"
    logger.info(f"Reading data from S3 path: {path}")
    dyf = glueContext.create_dynamic_frame.from_options(
        format_options={}, 
        connection_type="s3", 
        format="parquet", 
        connection_options={"paths": [path], "recurse": True}, 
        transformation_ctx=f"s3_source_{alias}"
    )
    logger.info(f"Extracted {dyf.count()} records from {table_name}")
    
    # Data Quality
    logger.info(f"Running data quality evaluation for {table_name}")
    EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": f"dq_{alias}",
            "enableDataQualityResultsPublishing": True
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        }
    )
    return dyf

def unpivot_dynamicframe(dyf: DynamicFrame, month_cols=None, new_name="unpivoted") -> DynamicFrame:
    """Unpivots monthly columns (V_ENE..V_DIC) to two columns: mes, monto."""
    month_cols = month_cols or ["V_ENE","V_FEB","V_MAR","V_ABR","V_MAY","V_JUN",
                                "V_JUL","V_AGO","V_SEP","V_OCT","V_NOV","V_DIC"]
    df = dyf.toDF()
    other_cols = [c for c in df.columns if c not in month_cols]
    expr_str = "stack({0}, {1}) as (mes, monto)".format(
        len(month_cols),
        ", ".join([f"'{i+1}', {c}" for i, c in enumerate(month_cols)])
    )
    df_unpivot = df.selectExpr(*other_cols, expr_str).withColumn("mes", col("mes").cast("int"))
    return DynamicFrame.fromDF(df_unpivot, glueContext, new_name)

def unpivot_monedas(dyf: DynamicFrame) -> DynamicFrame:
    """Unpivots Debe/Haber columns per currency into Moneda, Debe, Haber."""
    df = dyf.toDF()
    monedas = ["$", "UF", "USD", "EUR"]
    expr_parts = []
    for m in monedas:
        debe_col, haber_col = f"Debe_{m}", f"Haber_{m}"
        if debe_col in df.columns and haber_col in df.columns:
            expr_parts.append(f"'{m}', cast(`{debe_col}` as double), cast(`{haber_col}` as double)")
    if not expr_parts:
        raise ValueError("No currency columns found to unpivot")
    stack_expr = f"stack({len(expr_parts)}, {', '.join(expr_parts)}) as (Moneda, Debe, Haber)"
    other_cols = [c for c in df.columns if not c.startswith("Debe_") and not c.startswith("Haber_")]
    df_unpivot = df.select(*[col(c) for c in other_cols], expr(stack_expr))
    return DynamicFrame.fromDF(df_unpivot, glueContext, "unpivoted_monedas")

def clean_column_name(name: str) -> str:
    """Sanitize column names for Redshift."""
    name = re.sub(r'\W+', '_', name)
    name = re.sub(r'__+', '_', name).strip('_')
    if name.endswith('_$'):
        name = name[:-2] + '_USD'
    elif name.endswith('$'):
        name = name[:-1] + '_USD'
    if re.match(r'^\d', name):
        name = f"col_{name}"
    return name.lower()

def clean_dynamicframe_columns(dyf: DynamicFrame) -> DynamicFrame:
    df = dyf.toDF()
    df_clean = df.toDF(*[clean_column_name(c) for c in df.columns])
    return DynamicFrame.fromDF(df_clean, glueContext, "redshift_ready")

# ============================================================
# ETL Process
# ============================================================
# 1. Extract and unpivot data
sbo_diario = unpivot_monedas(extract_from_s3("SBO_CCP_POC_DIARIO", "sbo_diario"))
sbo_presupuesto = unpivot_dynamicframe(extract_from_s3("SBO_CCP_POC_PRESUPUESTO", "sbo_presupuesto"))
sbo_estructura = extract_from_s3("SBO_CCP_POC_ESTRUCTURA_EERR", "sbo_estructura").toDF().withColumnRenamed("Acctcode", "est_Acctcode")

inmmma_diario = unpivot_monedas(extract_from_s3("SBO_INMMMA_V1_POC_DIARIO", "inmmma_diario"))
inmmma_presupuesto = unpivot_dynamicframe(extract_from_s3("SBO_INMMMA_V1_POC_PRESUPUESTO", "inmmma_presupuesto"))
inmmma_estructura = extract_from_s3("SBO_INMMMA_V1_POC_ESTRUCTURA_EERR", "inmmma_estructura").toDF().withColumnRenamed("Acctcode", "est_Acctcode")

# 2. Join Diario with Presupuesto
def join_diario_presupuesto(diario_df, pres_df):
    pres_df = pres_df.toDF()
    pres_df = pres_df.select([col(c).alias(f"pre_{c}") for c in pres_df.columns])
    diario_df = diario_df.toDF()
    diario_df = diario_df.withColumn("Periodo", date_format(col("fecha"), "yyyy").cast("int")) \
                         .withColumn("mes", date_format(col("fecha"), "MM").cast("int"))
    join_cond = (
        (diario_df["Acctcode"] == pres_df["pre_acctcode"]) &
        (diario_df["Periodo"] == pres_df["pre_Periodo"]) &
        (diario_df["mes"] == pres_df["pre_mes"]) &
        (diario_df["Moneda"] == pres_df["pre_Moneda"]) &
        (coalesce(diario_df["cc1"], lit("0")) == coalesce(pres_df["pre_cc1"], lit("0"))) &
        (coalesce(diario_df["cc2"], lit("0")) == coalesce(pres_df["pre_cc2"], lit("0"))) &
        (coalesce(diario_df["cc3"], lit("0")) == coalesce(pres_df["pre_cc3"], lit("0"))) &
        (coalesce(diario_df["cc4"], lit("0")) == coalesce(pres_df["pre_cc4"], lit("0"))) &
        (coalesce(diario_df["cc5"], lit("0")) == coalesce(pres_df["pre_cc5"], lit("0")))
    )
    joined_df = diario_df.join(pres_df, join_cond, "left")
    return joined_df

joined_sbo = join_diario_presupuesto(sbo_diario, sbo_presupuesto)
joined_inmmma = join_diario_presupuesto(inmmma_diario, inmmma_presupuesto).dropDuplicates()

# 3. Join with Estructura
joined_sbo = joined_sbo.join(sbo_estructura, joined_sbo["Acctcode"] == sbo_estructura["est_Acctcode"], "left")
joined_inmmma = joined_inmmma.join(inmmma_estructura, joined_inmmma["Acctcode"] == inmmma_estructura["est_Acctcode"], "left")

# 4. Union datasets and cast columns
df_union = joined_sbo.unionByName(joined_inmmma)
for c, t in [("fecha", "timestamp"), ("debe", "decimal(18,2)"), ("haber", "decimal(18,2)"), ("pre_monto", "decimal(18,2)")]:
    df_union = df_union.withColumn(c, col(c).cast(t))
joined_dyf = DynamicFrame.fromDF(df_union, glueContext, "joined_dyf")

# 5. Sanitize column names
redshift_ready = clean_dynamicframe_columns(joined_dyf)
logger.info("Sanitized column names for Redshift")

# ============================================================
# Load to S3
# ============================================================
output_prefix = "SBO_CCP_POC_DIARIO_PRESUPUESTO"
output_path = f"s3://{target_bucket}/{output_prefix}/"
clear_s3_prefix(target_bucket, output_prefix)
glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": output_path, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="s3_target_node"
)
logger.info(f"Sanitized data written to S3 at {output_path}")

# ============================================================
# Load to Redshift
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

# Strict validation
sanitized_cols = [f.name for f in redshift_ready.schema().fields]
create_table_cols = [c.split()[0] for c in re.findall(r'(\w+)\s+\w+', preactions_sql)]
missing_cols = set(sanitized_cols) - set(create_table_cols)
if missing_cols:
    raise ValueError(f"Sanitized columns missing in Redshift table: {missing_cols}")

glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://{target_bucket}/aws-glue-assets/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.POC_JOIN",
        "connectionName": connection_name,
        "preactions": preactions_sql,
        "copyoptions": "FORMAT AS PARQUET"
    },
    transformation_ctx="redshift_node"
)
logger.info("Data successfully written to Redshift table: public.POC_JOIN")

# ============================================================
# Commit Glue Job
# ============================================================
job.commit()
logger.info(f"Glue job {job_name} completed successfully.")
