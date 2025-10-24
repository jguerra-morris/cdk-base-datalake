import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# Parse Job Parameters
# ============================================================
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET", "CONNECTION_NAME", "REDSHIFT_TABLE"]
)
job_name = args["JOB_NAME"]
source_bucket = args["SOURCE_BUCKET"]
target_bucket = args["TARGET_BUCKET"]

logger.info(f"Starting AWS Glue Job: {job_name}")

# ============================================================
# Initialize Glue Context & Spark
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

# ============================================================
# Default Data Quality Ruleset
# ============================================================
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""
logger.info("Default Data Quality ruleset configured successfully.")

# ============================================================
# Configuration Constants
# ============================================================
SOURCE_S3_PATH = "s3://"+source_bucket
TARGET_S3_PATH = "s3://"+target_bucket+"/"
TEMP_REDSHIFT_DIR = "s3://aws-glue-assets-331702097493-us-east-1/temporary/"
REDSHIFT_CONNECTION =  args["CONNECTION_NAME"]
REDSHIFT_TABLE = args["REDSHIFT_TABLE"]

logger.info(f"Source S3 path: {SOURCE_S3_PATH}")
logger.info(f"Target S3 path: {TARGET_S3_PATH}")
logger.info(f"Redshift table: {REDSHIFT_TABLE}")

# ============================================================
# Helper Functions
# ============================================================

def extract_from_s3(path: str) -> DynamicFrame:
    """
    Extract data from S3 in Parquet format and return as a DynamicFrame.
    """
    logger.info(f"Reading data from S3: {path}")
    frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [path]},
        transformation_ctx="extract_s3_source"
    )
    logger.info(f"Extracted {frame.count()} records from {path}")
    return frame


def evaluate_data_quality(frame: DynamicFrame) -> None:
    """
    Evaluate basic data quality rules on the given DynamicFrame.
    """
    logger.info("Running data quality checks...")
    EvaluateDataQuality().process_rows(
        frame=frame,
        ruleset=DEFAULT_DATA_QUALITY_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "DQ_Evaluation_Context",
            "enableDataQualityResultsPublishing": True
        },
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
            "observations.scope": "ALL"
        }
    )
    logger.info("Data quality evaluation completed.")


def write_to_s3(frame: DynamicFrame, output_path: str) -> None:
    """
    Write the DynamicFrame to an S3 path in Glue Parquet format.
    """
    logger.info(f"Writing data to target S3 path: {output_path}")
    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={"path": output_path, "partitionKeys": []},
        format_options={"compression": "snappy"},
        transformation_ctx="write_s3_output"
    )
    logger.info("Successfully wrote data to S3.")


def load_to_redshift(frame: DynamicFrame) -> None:
    """
    Load the DynamicFrame into a Redshift table.
    """
    logger.info(f"Loading data into Redshift table: {REDSHIFT_TABLE}")

    preactions_sql = """
        CREATE TABLE IF NOT EXISTS public.banmedica_test (
        periodo VARCHAR, 
        afil_nrut VARCHAR, 
        id_afiliado VARCHAR, 
        isap_cempresa VARCHAR, 
        cargas VARCHAR, 
        beneficiarios VARCHAR, 
        fecha_nacimiento VARCHAR, 
        edad VARCHAR, 
        sexo VARCHAR, 
        nicho VARCHAR, 
        rut_compensado VARCHAR, 
        tipo_trabajador VARCHAR, 
        region VARCHAR, 
        localidad VARCHAR, 
        comuna VARCHAR, 
        ccosto VARCHAR, 
        fecha_ingreso_isapre VARCHAR, 
        estado VARCHAR, 
        tramo_cot VARCHAR, 
        afecto_prima_ext VARCHAR, 
        adecuacion_7p VARCHAR, 
        tiene_devolucion VARCHAR, 
        devolucion_total VARCHAR, 
        margen_am VARCHAR, 
        meses_morosidad VARCHAR, 
        morosidad_total VARCHAR, 
        factor_grupal VARCHAR, 
        factor_nuevo VARCHAR, 
        valor_base VARCHAR, 
        pactado_total VARCHAR, 
        pactado VARCHAR, 
        precio_plan VARCHAR, 
        dif_fallo VARCHAR, 
        prima_menores VARCHAR, 
        prima_ges VARCHAR, 
        prima_caec VARCHAR, 
        benef_adic VARCHAR, 
        ajuste7 VARCHAR, 
        prima_extra VARCHAR, 
        compensa_negativa VARCHAR, 
        compensa_positiva VARCHAR, 
        a_pagar VARCHAR, 
        renta_imponible_prom VARCHAR, 
        ind_mov_fact_grupo VARCHAR, 
        plan VARCHAR, 
        tp_tramo VARCHAR, 
        tipo_plan VARCHAR, 
        modalidad_plan VARCHAR, 
        bonif_alemana_amb VARCHAR, 
        bonif_alemana_hosp VARCHAR, 
        bonif_clc_amb VARCHAR, 
        bonif_clc_hosp VARCHAR, 
        bonif_csm_amb VARCHAR, 
        bonif_csm_hosp VARCHAR, 
        bonif_davila_amb VARCHAR, 
        bonif_davila_hosp VARCHAR, 
        bonif_hosp_uc_amb VARCHAR, 
        bonif_hosp_uc_hosp VARCHAR, 
        bonif_indisa_amb VARCHAR, 
        bonif_indisa_hosp VARCHAR, 
        bonif_integramedica_amb VARCHAR, 
        bonif_san_carlos_amb VARCHAR, 
        bonif_san_carlos_hosp VARCHAR, 
        bonif_tabancura_amb VARCHAR, 
        bonif_tabancura_hosp VARCHAR, 
        bonif_uandes_amb VARCHAR, 
        bonif_uandes_hosp VARCHAR, 
        bonif_vespucio_amb VARCHAR, 
        bonif_vespucio_hosp VARCHAR, 
        bonif_vidaintegra_amb VARCHAR, 
        bonif_otros_amb VARCHAR, 
        bonif_otros_hosp VARCHAR, 
        bonif_libre_eleccion_amb VARCHAR, 
        bonif_libre_eleccion_hosp VARCHAR, 
        cant_prest_pref_amb VARCHAR, 
        cant_prest_pref_hosp VARCHAR, 
        max_pref_amb VARCHAR, 
        max_pref_hosp VARCHAR, 
        cobertura_parto VARCHAR, 
        familia_plan VARCHAR, 
        fecha_creacion_plan VARCHAR, 
        fecha_fin_plan VARCHAR, 
        tiene_oni VARCHAR, 
        cant_prod_regalo VARCHAR, 
        cant_prod_venta VARCHAR, 
        catastrofico_regalo VARCHAR, 
        catastrofico_venta VARCHAR, 
        cesantia_regalo VARCHAR, 
        cesantia_venta VARCHAR, 
        farmacia_regalo VARCHAR, 
        farmacia_venta VARCHAR, 
        medy_regalo VARCHAR, 
        medy_venta VARCHAR, 
        omt_regalo VARCHAR, 
        omt_venta VARCHAR, 
        pack_regalo VARCHAR, 
        pack_venta VARCHAR, 
        aumento_cob_regalo VARCHAR, 
        aumento_cob_venta VARCHAR, 
        otro_regalo VARCHAR, 
        otro_venta VARCHAR, 
        preexistencia VARCHAR, 
        dias_autor_hist VARCHAR, 
        dias_autor_per VARCHAR, 
        dias_solic_hist VARCHAR, 
        dias_solic_per VARCHAR, 
        lic_rechazadas_historico VARCHAR, 
        lic_rechazadas_periodo VARCHAR, 
        lic_reducidas_historico VARCHAR, 
        lic_reducidas_periodo VARCHAR, 
        licencias_historico VARCHAR, 
        licencias_periodo VARCHAR, 
        vbonif_amb_hist VARCHAR, 
        vbonif_amb_per VARCHAR, 
        vbonif_hosp_hist VARCHAR, 
        vbonif_hosp_per VARCHAR, 
        vcant_amb_hist VARCHAR, 
        vcant_amb_per VARCHAR, 
        vcant_hosp_hist VARCHAR, 
        vcant_hosp_per VARCHAR, 
        vfac_amb_hist VARCHAR, 
        vfac_amb_per VARCHAR, 
        vfac_hosp_hist VARCHAR, 
        vfac_hosp_per VARCHAR, 
        tasa_amb_hist VARCHAR, 
        tasa_amb_per VARCHAR, 
        tasa_hosp_hist VARCHAR, 
        tasa_hosp_per VARCHAR, 
        caso_historico VARCHAR, 
        caso_periodo VARCHAR, 
        crm01_historico VARCHAR, 
        crm01_periodo VARCHAR, 
        crm02_historico VARCHAR, 
        crm02_periodo VARCHAR, 
        crm03_historico VARCHAR, 
        crm03_periodo VARCHAR, 
        crm04_historico VARCHAR, 
        crm04_periodo VARCHAR, 
        crm05_historico VARCHAR, 
        crm05_periodo VARCHAR, 
        crm06_historico VARCHAR, 
        crm06_periodo VARCHAR, 
        crm07_historico VARCHAR, 
        crm07_periodo VARCHAR, 
        crm08_historico VARCHAR, 
        crm08_periodo VARCHAR, 
        crm09_historico VARCHAR, 
        crm09_periodo VARCHAR, 
        crm10_historico VARCHAR, 
        crm10_periodo VARCHAR, 
        destino_fuga VARCHAR, 
        movilidad_reingreso VARCHAR, 
        reclamo_historico VARCHAR, 
        reclamo_periodo VARCHAR, 
        nps_ambulatorio VARCHAR, 
        nps_asistencia_en_linea VARCHAR, 
        nps_relacional VARCHAR, 
        nps_beneficios_adicionales VARCHAR, 
        nps_contact_center VARCHAR, 
        nps_ges VARCHAR, 
        nps_hospitalario VARCHAR, 
        nps_licencias_medicas VARCHAR, 
        nps_modificacion_contrato VARCHAR, 
        nps_paginaweb VARCHAR, 
        nps_sanas VARCHAR, 
        nps_telemedicina VARCHAR, 
        nps_ventas VARCHAR, 
        nps_widget VARCHAR, 
        ultimo_nps VARCHAR, 
        historico_nps VARCHAR, 
        csat_ambulatorio VARCHAR, 
        csat_asistencia_en_linea VARCHAR, 
        csat_relacional VARCHAR, 
        csat_beneficios_adicionales VARCHAR, 
        csat_contact_center VARCHAR, 
        csat_ges VARCHAR, 
        csat_hospitalario VARCHAR, 
        csat_licencias_medicas VARCHAR, 
        csat_modificacion_contrato VARCHAR, 
        csat_paginaweb VARCHAR, 
        csat_sanas VARCHAR, 
        csat_telemedicina VARCHAR, 
        csat_ventas VARCHAR, 
        csat_widget VARCHAR, 
        ultimo_isat VARCHAR, 
        cbi_relacional VARCHAR, 
        historico_isat VARCHAR, 
        frecuencia VARCHAR, 
        recencia VARCHAR, 
        atencion_online_periodo VARCHAR, 
        atencion_telefonica_periodo VARCHAR, 
        atencion_presencial_periodo VARCHAR, 
        tiempo_gestion VARCHAR, 
        fecha_proceso VARCHAR);
        TRUNCATE TABLE public.banmedica_test;
    """

    glue_context.write_dynamic_frame.from_options(
        frame=frame,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": TEMP_REDSHIFT_DIR,
            "useConnectionProperties": "true",
            "dbtable": REDSHIFT_TABLE,
            "connectionName": REDSHIFT_CONNECTION,
            "preactions": preactions_sql
        },
        transformation_ctx="load_redshift_target"
    )
    logger.info("Data successfully loaded into Redshift.")


# ============================================================
# Main ETL Flow
# ============================================================
try:
    # 1️⃣ Extract
    s3_source_frame = extract_from_s3(SOURCE_S3_PATH)

    # 2️⃣ Data Quality Check
    evaluate_data_quality(s3_source_frame)

    # 3️⃣ Write to S3 (Transformed Layer)
    write_to_s3(s3_source_frame, TARGET_S3_PATH)

    # 4️⃣ Load to Redshift
    load_to_redshift(s3_source_frame)

    logger.info(f"Glue Job {job_name} completed successfully!")

except Exception as e:
    logger.error(f"Job {job_name} failed: {str(e)}", exc_info=True)
    raise e

finally:
    job.commit()
