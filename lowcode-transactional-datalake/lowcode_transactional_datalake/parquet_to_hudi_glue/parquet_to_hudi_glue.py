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
    "TARGET_DATABASE",
    "TARGET_TABLE",
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
SOURCE_KEY = args["SOURCE_KEY"]
TARGET_BUCKET = args["TARGET_BUCKET"]
TARGET_DATABASE = args["TARGET_DATABASE"]
TARGET_TABLE = args["TARGET_TABLE"]

# Script generated for node Amazon S3
AmazonS3_node1659003728957 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"s3://{SOURCE_BUCKET}/{SOURCE_KEY}"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1659003728957",
)

# Script generated for node Apply Mapping
ApplyMapping_node1659003779551 = ApplyMapping.apply(
    frame=AmazonS3_node1659003728957,
    mappings=[
        ("id", "string", "id", "string"),
        ("tx_serial_number", "string", "tx_serial_number", "long"),
        ("item_name", "string", "item_name", "string"),
        ("sku", "string", "sku", "long"),
        ("price", "string", "price", "double"),
        ("remark", "string", "remark", "long"),
        ("created_at", "string", "created_at", "string"),
        ("updated_at", "string", "updated_at", "string"),
    ],
    transformation_ctx="ApplyMapping_node1659003779551",
)

# Script generated for node Apache Hudi Connector 0.10.1 for AWS Glue 3.0
ApacheHudiConnector0101forAWSGlue30_node1658759738947 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1659003779551,
    connection_type="marketplace.spark",
    connection_options={
        "className": "org.apache.hudi",
        "connectionName": "hudi-connector",
        "hoodie.bulkinsert.shuffle.parallelism": "8",
        "hoodie.consistency.check.enabled": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": TARGET_DATABASE,
        "hoodie.datasource.hive_sync.table": TARGET_TABLE,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "tx_serial_number",
        "hoodie.datasource.write.precombine.field": "updated_at",
        "hoodie.table.name": TARGET_TABLE,
        "path": f"s3://{TARGET_BUCKET}/{TARGET_DATABASE}/{TARGET_TABLE}",
    },
    transformation_ctx="ApacheHudiConnector0101forAWSGlue30_node1658759738947",
)

job.commit()
