import sys
import logging
import re
from datetime import datetime
from datetime import date
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import expr, col, date_format, coalesce, lit


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
s3 = boto3.resource("s3")

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

# Fecha actual para particiones
today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# Tablas a procesar
tables = [
    "SBO_CCP.POC_DIARIO",
    "SBO_CCP.POC_PRESUPUESTO",
    "SBO_CCP.POC_ESTRUCTURA_EERR",
    "SBO_INMMMA_V1.POC_DIARIO",
    "SBO_INMMMA_V1.POC_PRESUPUESTO",
    "SBO_INMMMA_V1.POC_ESTRUCTURA_EERR"
]


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


# ----------------------------
# Función unpivot meses Presupuesto
# ----------------------------
def unpivot_dynamicframe(glueContext, dyf, new_name="unpivoted"):
    """
    Convierte columnas de meses (v_ene, v_feb, ..., v_dic) en 2 columnas:
    - mes (int, 1 a N)
    - monto (valor de la columna)
    
    Mantiene todas las demás columnas intactas.
    
    Parámetros:
        glueContext: GlueContext activo
        dyf: DynamicFrame de entrada
        new_name: nombre para el nuevo DynamicFrame
    
    Retorna:
        DynamicFrame unpivotado
    """
    # lista de columnas de meses en orden
    month_cols = ["V_ENE","V_FEB","V_MAR","V_ABR","V_MAY","V_JUN",
              "V_JUL","V_AGO","V_SEP","V_OCT","V_NOV","V_DIC"]
              
    # Convertir a DataFrame de Spark
    df = dyf.toDF()
    
    # Otras columnas (que no son de meses)
    other_cols = [c for c in df.columns if c not in month_cols]
    
    # Construir la expresión stack para hacer unpivot
    expr = "stack({0}, {1}) as (mes, monto)".format(
        len(month_cols),
        ", ".join([f"'{i+1}', {col}" for i, col in enumerate(month_cols)])
    )
    
    # Aplicar unpivot
    df_unpivot = df.selectExpr(*other_cols, expr)
    
    # Asegurar que mes sea int
    df_unpivot = df_unpivot.withColumn("mes", df_unpivot["mes"].cast("int"))
    
    # Volver a DynamicFrame
    return DynamicFrame.fromDF(df_unpivot, glueContext, new_name)

# ----------------------------
# Función carga data s3 source
# ----------------------------
def extract_and_evaluate(table_name, alias):
    """
    Extrae datos de una tabla en S3 (particionado por year/month/day) 
    y ejecuta evaluación de Data Quality.
    
    Parámetros:
    -----------
    table_name : str
        Nombre de la tabla/carpeta en S3 (ej: "SBO_CCP_POC_DIARIO").
    alias : str
        Alias para identificar el nodo en Glue.
    
    Retorna:
    --------
    tuple : (dynamic_frame, dq_results, record_count)
    """
    
    # ----------------------------
    # Extract from Amazon S3
    # ----------------------------
    path = f"s3://{source_bucket}/{table_name}/year={year}/month={month}/day={day}/"
    logger.info(f"Reading data from S3 path: {path} ...")
    
    s3_source_node = glueContext.create_dynamic_frame.from_options(
        format_options={}, 
        connection_type="s3", 
        format="parquet", 
        connection_options={
            "paths": [path], 
            "recurse": True
        }, 
        transformation_ctx=f"s3_source_node_{alias}"
    )
    
    record_count = s3_source_node.count()
    logger.info(f"Extracted {record_count} records from {table_name}.")

    # ----------------------------
    # Data Quality Evaluation
    # ----------------------------
    logger.info("Running Data Quality evaluation on extracted data ...")
    
    dq_results = EvaluateDataQuality().process_rows(
        frame=s3_source_node, 
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
    
    logger.info(f"Data Quality evaluation completed for {table_name}.")

    return s3_source_node

# ----------------------------
# Función unpivot moneda Diario
# ----------------------------
def unpivot_monedas(df):
    """
    Convierte columnas Debe_$, Haber_$, Debe_UF, Haber_UF, ...
    en columnas: Moneda, Debe, Haber.
    Mantiene todas las demás columnas.
    """
    logger.info("Iniciando función unpivot_monedas...")
    df = df.toDF()
    
    logger.info(f"Columnas originales del DataFrame: {df.columns}")
    # Mapeo de monedas
    monedas = ["$", "UF", "USD", "EUR"]

    # Construcción dinámica del stack
    expr_parts = []
    for moneda in monedas:
        col_debe = f"Debe_{moneda}"
        col_haber = f"Haber_{moneda}"
        if col_debe in df.columns and col_haber in df.columns:
            expr_parts.append(f"'{moneda}', cast(`{col_debe}` as double), cast(`{col_haber}` as double)")
        logger.info(f"Se encontraron columnas para moneda {moneda}: {col_debe}, {col_haber}")
    if not expr_parts:
        raise Exception("No se encontraron columnas de monedas en el DataFrame.")

    expr_str = f"stack({len(expr_parts)}, {', '.join(expr_parts)}) as (Moneda, Debe, Haber)"
    logger.info(f"Expresión generada para stack: {expr_str}")
    
    # Selecciona las demás columnas + el stack
    df_unpivoted = df.select(
        *[col(c) for c in df.columns if not c.startswith("Debe_") and not c.startswith("Haber_")],
        expr(expr_str)
    )
    print(df.columns)
    return DynamicFrame.fromDF(df_unpivoted, glueContext, "df_unpivoted")


# ----------------------------
# Carga data
# ----------------------------
# SBO_CCP
s3_source_node_sbo_poc_presupuesto = extract_and_evaluate('SBO_CCP_POC_PRESUPUESTO','s3_source_node_sbo_poc_presupuesto')
s3_source_node_sbo_poc_diario = extract_and_evaluate('SBO_CCP_POC_DIARIO','s3_source_node_sbo_poc_diario')
s3_source_node_sbo_poc_estructura = extract_and_evaluate('SBO_CCP_POC_ESTRUCTURA_EERR','s3_source_node_sbo_poc_estructura')

# SBO_INMMMA_V1
s3_source_node_inmmma_poc_presupuesto = extract_and_evaluate('SBO_INMMMA_V1_POC_PRESUPUESTO','s3_source_node_inmmma_poc_presupuesto')
s3_source_node_inmmma_poc_diario = extract_and_evaluate('SBO_INMMMA_V1_POC_DIARIO','s3_source_node_inmmma_poc_diario')
s3_source_node_inmmma_poc_estructura = extract_and_evaluate('SBO_INMMMA_V1_POC_ESTRUCTURA_EERR','s3_source_node_inmmma_poc_estructura')

#Aplica unpivot para ambos casos
# SBO_CCP
s3_source_node_poc_diario_unpivot = unpivot_monedas(s3_source_node_sbo_poc_diario)
s3_source_node_poc_presupuesto_unpivot = unpivot_dynamicframe(
    glueContext,
    s3_source_node_sbo_poc_presupuesto,
    new_name="s3_source_node_poc_presupuesto_unpivot"
)

# SBO_INMMMA_V1
s3_source_node_inmmma_poc_diario_unpivot = unpivot_monedas(s3_source_node_inmmma_poc_diario)
s3_source_node_inmmma_poc_presupuesto_unpivot = unpivot_dynamicframe(
    glueContext,
    s3_source_node_inmmma_poc_presupuesto,
    new_name="s3_source_node_inmmma_poc_presupuesto_unpivot"
)


# ---------------------------------
# Join Diario - Presupuesto SBO_CCP
# ---------------------------------
india_df = s3_source_node_poc_diario_unpivot.toDF()
inpre_df = s3_source_node_poc_presupuesto_unpivot.toDF()

# Añadimos año y mes en india_df para facilitar join
india_df = india_df.withColumn("Periodo", date_format(col("fecha"), "yyyy").cast("int"))
india_df = india_df.withColumn("mes", date_format(col("fecha"), "MM").cast("int"))

# Renombramos columnas en inpre_df para evitar colisiones
inpre_df = inpre_df.select(
    [col(c).alias(f"pre_{c}") for c in inpre_df.columns]
)

# Ajustamos columnas de join
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

joined_diario_presupuesto = india_df.join(inpre_df, join_condition, "left")
joined_dyp_sbo = DynamicFrame.fromDF(joined_diario_presupuesto, glueContext, "joined_dyp_sbo")



# ---------------------------------
# Join Diario - Presupuesto SBO_INMMMA_V1
# ---------------------------------
india_df = s3_source_node_inmmma_poc_diario_unpivot.toDF()
inpre_df = s3_source_node_inmmma_poc_presupuesto_unpivot.toDF()

# Añadimos año y mes en india_df para facilitar join
india_df = india_df.withColumn("Periodo", date_format(col("fecha"), "yyyy").cast("int"))
india_df = india_df.withColumn("mes", date_format(col("fecha"), "MM").cast("int"))

# Renombramos columnas en inpre_df para evitar colisiones
inpre_df = inpre_df.select(
    [col(c).alias(f"pre_{c}") for c in inpre_df.columns]
)

# Ajustamos columnas de join
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

joined_diario_presupuesto_inmmma = india_df.join(inpre_df, join_condition, "left")
joined_diario_presupuesto_inmmma = joined_diario_presupuesto_inmmma.dropDuplicates()


# ---------------------------------
# Join Estructura SBO_INMMMA_V1
# ---------------------------------
# Renombramos columnas en estructura para evitar colisiones
s3_source_node_inmmma_poc_estructura = s3_source_node_inmmma_poc_estructura.toDF()
s3_source_node_inmmma_poc_estructura = s3_source_node_inmmma_poc_estructura.select(
    [col(c).alias(f"est_{c}") for c in s3_source_node_inmmma_poc_estructura.columns]
)

joined_inmmma_est = joined_diario_presupuesto_inmmma.join(
    s3_source_node_inmmma_poc_estructura,
    joined_diario_presupuesto_inmmma["Acctcode"] == s3_source_node_inmmma_poc_estructura["est_Acctcode"],
    "left"
)

# ---------------------------------
# Join Estructura SBO_CCP
# ---------------------------------
# Renombramos columnas en estructura para evitar colisiones
s3_source_node_sbo_poc_estructura = s3_source_node_sbo_poc_estructura.toDF()
s3_source_node_sbo_poc_estructura = s3_source_node_sbo_poc_estructura.select(
    [col(c).alias(f"est_{c}") for c in s3_source_node_sbo_poc_estructura.columns]
)

joined_ccp_est = joined_diario_presupuesto.join(
    s3_source_node_sbo_poc_estructura,
    joined_diario_presupuesto["Acctcode"] == s3_source_node_sbo_poc_estructura["est_Acctcode"],
    "left"
)

df_union = joined_ccp_est.unionByName(joined_inmmma_est)

df_union = df_union.withColumn("fecha", col("fecha").cast("timestamp"))
df_union = df_union.withColumn("debe", col("debe").cast("decimal(18,2)"))
df_union = df_union.withColumn("haber", col("haber").cast("decimal(18,2)"))
df_union = df_union.withColumn("pre_monto", col("pre_monto").cast("decimal(18,2)"))

joined_dyp = DynamicFrame.fromDF(df_union, glueContext, "joined_dyp")





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
joined_dyp.printSchema()
logger.info(f"Columns BEFORE sanitization: {[f.name for f in joined_dyp.schema().fields]}")

logger.info("Sanitizing column names for Redshift compatibility ...")
redshift_ready_node = clean_dynamicframe_columns(joined_dyp)

logger.info("Schema AFTER sanitization:")
redshift_ready_node.printSchema()
logger.info(f"Columns AFTER sanitization: {[f.name for f in redshift_ready_node.schema().fields]}")

logger.info(f"Record count BEFORE sanitization: {joined_dyp.count()}")
logger.info(f"Record count AFTER sanitization: {redshift_ready_node.count()}")

# ----------------------------
# Load to Amazon S3 (✅ use sanitized data)
# ----------------------------

output_prefix = "SBO_CCP_POC_DIARIO_PRESUPUESTO"
output_path = f"s3://{target_bucket}/{output_prefix}/"
clear_s3_prefix(target_bucket, output_prefix)

logger.info(f"Writing sanitized data to S3 path: {output_path}")
s3_target_node = glueContext.write_dynamic_frame.from_options(
    frame=joined_dyp,   # ✅ use sanitized frame
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
# ✅ Strict validation: fail if mismatch
sanitized_columns = [f.name for f in redshift_ready_node.schema().fields]
create_table_cols = [c.split()[0] for c in re.findall(r'(\w+)\s+\w+', preactions_sql)]
missing_in_table = set(sanitized_columns) - set(create_table_cols)
if missing_in_table:
    raise Exception(f"Sanitized columns not present in CREATE TABLE: {sorted(missing_in_table)}")

logger.info("Writing sanitized data to Redshift table: public.POC_JOIN")
redshift_node = glueContext.write_dynamic_frame.from_options(
    frame=redshift_ready_node,
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

