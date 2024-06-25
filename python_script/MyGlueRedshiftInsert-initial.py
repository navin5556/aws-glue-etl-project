import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1719324996584 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://myglue-etl-project-sit/output/"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1719324996584")

# Script generated for node Change Schema
ChangeSchema_node1719324998248 = ApplyMapping.apply(
    frame=AmazonS3_node1719324996584, 
    mappings=[
        ("new_year", "string", "new_year", "string"), 
        ("cnt", "long", "cnt", "long"), 
        ("qty", "long", "qty", "long")
        ], 
        transformation_ctx="ChangeSchema_node1719324998248")

# Script generated for node Amazon Redshift
AmazonRedshift_node1719325012344 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1719324998248, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-935677405004-ap-south-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "public.product_tab_def", 
        "connectionName": "myredshiftcluster-connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (new_year VARCHAR, cnt BIGINT, qty BIGINT);"}, 
        transformation_ctx="AmazonRedshift_node1719325012344")

job.commit()