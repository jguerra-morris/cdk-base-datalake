import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node SAP HANA
sapNode = glueContext.create_dynamic_frame.from_options(
    connection_type="saphana",
    connection_options={"connectionName": "test-15-09-02", "dbtable": "SBO_CCP.POC_DIARIO"},
    transformation_ctx="sapNode"
    )

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=sapNode,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dqNode",
        "enableDataQualityResultsPublishing": True},
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"}
    )

s3Node = glueContext.write_dynamic_frame.from_options(
    frame=sapNode,
    connection_type="s3", format="glueparquet",
    connection_options={
        "path": "s3://myproj-us-east-1-bucket-dev-datalake-raw-679835924785",
        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="s3Node"
    )

job.commit()