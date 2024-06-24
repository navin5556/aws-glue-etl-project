import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1719241510194 = glueContext.create_dynamic_frame.from_catalog(
    database="mydatabase", 
    table_name="product", 
    transformation_ctx="AmazonS3_node1719241510194"
    )

# Script generated for node Change Schema
ChangeSchema_node1719241515625 = ApplyMapping.apply(
    frame=AmazonS3_node1719241510194, 
    mappings=[
        ("marketplace", "string", "marketplace", "string"), 
        ("customer_id", "long", "customer_id", "long"), 
        ("product_id", "string", "product_id", "string"), 
        ("seller_id", "string", "seller_id", "string"), 
        ("sell_date", "string", "sell_date", "string"), 
        ("quantity", "long", "quantity", "long"), 
        ("year", "string", "year", "string")
        ], 
        transformation_ctx="ChangeSchema_node1719241515625"
        )

# Script generated for node Amazon S3
AmazonS3_node1719241517553 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1719241515625, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": "s3://myglue-etl-project-sit/output/", "partitionKeys": []}, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_node1719241517553")

job.commit()