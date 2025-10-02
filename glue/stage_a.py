import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Glue/Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================
# Read Input Data
# =============================

# Source 1: Delitos Arequipa
delitos_arequipa_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://demo-us-east-1-bucket-dev-datalake-raw-539548017896/delitos_arequipa/"],
        "recurse": True
    },
    transformation_ctx="delitos_arequipa_dyf"
)

# Source 2: Delitos Junín
delitos_junin_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://demo-us-east-1-bucket-dev-datalake-raw-539548017896/rdsdb_delitos_junin/"],
        "recurse": True
    },
    transformation_ctx="delitos_junin_dyf"
)

# =============================
# Transformations
# =============================

# Concatenate the two datasets (same schema)
combined_delitos_dyf = delitos_arequipa_dyf.union(delitos_junin_dyf)

# =============================
# Write Output
# =============================

glueContext.write_dynamic_frame.from_options(
    frame=combined_delitos_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://demo-us-east-1-bucket-dev-datalake-master-539548017896/delitos_all/",
        "partitionKeys": []   # keep empty to preserve original structure
    },
    transformation_ctx="s3_output_combined_delitos"
)

# Commit Job
job.commit()
