import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "SOURCE_KEY",
    "TARGET_BUCKET",
    "TARGET_PREFIX",
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
SOURCE_KEY = args["SOURCE_KEY"]
TARGET_BUCKET = args["TARGET_BUCKET"]
TARGET_PREFIX = args["TARGET_PREFIX"]

# Script generated for node Amazon S3
AmazonS3_node1659000953397 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1659000953397",
)

# Script generated for node Amazon S3
AmazonS3_node1659000956531 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1659000953397,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1659000956531",
)

job.commit()
