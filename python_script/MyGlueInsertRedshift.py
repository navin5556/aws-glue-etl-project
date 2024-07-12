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
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://myglue-etl-project-s3-bucket/Output/"], "recurse": True}, 
    transformation_ctx="S3bucket_node1"
    )

# Script generated for node Change Schema
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1, 
    mappings=[
        ("new_year", "string", "year", "string"), 
        ("cnt", "long", "noofcustomer", "BIGINT"), 
        ("qty", "long", "quantity", "BIGINT")
        ], 
    transformation_ctx="ApplyMapping_node2"
    )

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-535560865181-ap-south-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "public.product_tab_def", 
        "connectionName": "myredshiftcluster-connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (new_year VARCHAR, cnt BIGINT, qty BIGINT);"
        }, 
        transformation_ctx="AmazonRedshift_node3"
        )

job.commit()