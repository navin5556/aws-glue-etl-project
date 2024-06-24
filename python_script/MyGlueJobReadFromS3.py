import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import logging


# you can use any name instead of my_logger when creating a logger. The name you choose is primarily for identification purposes, 
#... especially useful when you have multiple loggers in your application and want to distinguish between them.

#This creates a logger named my_logger and sets its logging level to INFO. This means the logger will capture all 
#... messages at the INFO level and above (e.g., WARNING, ERROR, CRITICAL).
logger = logging.getLogger('my_logger') 
logger.setLevel(logging.INFO)

# Create a handler for CloudWatch (or console in this case)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info('My log message')


#this setup code ensures that the Glue job is properly configured and integrated with AWS Glue and Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Glue Data Catalog is a central repository to store metadata about data sources, which include the schema, 
# ..location (like an S3 bucket path), and other information.
# DynamicFrames are a key component of AWS Glue and offer several advantages for ETL operations.
#This line of code loads data from the 'product' table of 'mydatabase' database into a DynamicFrame named S3bucket_node1.
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mydatabase", 
    table_name="product", 
    transformation_ctx="S3bucket_node1"
)

logger.info('print schema of S3bucket_node1')
S3bucket_node1.printSchema()

count = S3bucket_node1.count()
print("Number of rows in S3bucket_node1 dynamic frame: ",count)
logger.info('count for frame is {}'.format(count))

# transformation_ctx="S3bucket_node1": This is a unique string that is used to track artifacts and errors in AWS Glue. 
# ...It’s like a label for this particular operation.

# Script generated for node Change Schema
# The second part of the code is using the ApplyMapping.apply method to change the schema of the DynamicFrame.
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1, 
    mappings=[
        ("marketplace", "string", "new_marketplace", "string"), 
        ("customer_id", "long", "new_customer_id", "long"), 
        ("product_id", "string", "new_product_id", "string"), 
        ("seller_id", "string", "new_seller_id", "string"), 
        ("sell_date", "string", "new_sell_date", "string"), 
        ("quantity", "long", "new_quantity", "long"), 
        ("year", "string", "new_year", "string")
        ], 
    transformation_ctx="ApplyMapping_node2")

#convert those string values to long values using the resolveChoice transform method with a cast:long option:
#This replaces the string values with null values

ResolveChoice_node = ApplyMapping_node2.resolveChoice(specs = [('new_seller_id','cast:long')],
transformation_ctx="ResolveChoice_node"
)


logger.info('print schema of ResolveChoice_node')
ResolveChoice_node.printSchema()

#convert dynamic dataframe into spark dataframe to leverage Spark’s built-in functions.
logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
spark_data_frame=ResolveChoice_node.toDF()
spark_data_frame.show()

#apply spark where clause
logger.info('filter rows with  where new_seller_id is not null')
spark_data_frame_filter = spark_data_frame.where("new_seller_id is NOT NULL")
spark_data_frame.show(n=1000, truncate=False)

# A new column ‘new_status’ is added to the DataFrame with a constant value ‘Active’, all rows have the value “Active”
logger.info('create new column status with Active value')
spark_data_frame_filter = spark_data_frame_filter.withColumn("new_status", lit("Active"))
spark_data_frame_filter.show()

#The DataFrame is registered as a temporary table view ‘product_view’ for running SQL queries.
logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
spark_data_frame_filter.createOrReplaceTempView("product_view")

#A SQL query is run on the ‘product_view’ to group by ‘new_year’ and calculate the count of ‘new_customer_id’ and the sum of ‘new_quantity’.
logger.info('create dataframe by spark sql ')
product_sql_df = spark.sql("SELECT new_year,count(new_customer_id) as cnt,sum(new_quantity) as qty FROM product_view group by new_year ")

logger.info('display records after aggregate result')
product_sql_df.show()

# Convert the data frame back to a dynamic frame
logger.info('convert spark dataframe to dynamic frame ')
dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")


logger.info('dynamic frame uploaded in bucket myglue-etl-project/output/newproduct/ in parquet format ')
# Script generated for node Amazon S3
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    # frame=ApplyMapping_node2, 
    frame=dynamic_frame, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": "s3://myglue-etl-project-sit/output/", "partitionKeys": []}, 
    format_options={"compression": "uncompressed"}, 
    transformation_ctx="S3bucket_node3")

logger.info('etl job processed successfully')

job.commit()