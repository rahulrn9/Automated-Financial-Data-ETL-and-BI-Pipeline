import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'DEST_REDSHIFT_TABLE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.json(f"s3://{args['SOURCE_BUCKET']}/intraday/")
from pyspark.sql.functions import col, to_timestamp, lag, when
from pyspark.sql.window import Window

df = df.dropDuplicates().withColumn("timestamp", to_timestamp(col("timestamp")))
window = Window.orderBy("timestamp")
df = df.withColumn("price_change",
                   when(lag("price").over(window).isNotNull(),
                        (col("price") - lag("price").over(window)) / lag("price").over(window)).otherwise(0.0))

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(df, glueContext, "df"),
    catalog_connection="redshift-jdbc",
    connection_options={"dbtable": args['DEST_REDSHIFT_TABLE'], "database": "analytics"},
    redshift_tmp_dir=f"s3://{args['SOURCE_BUCKET']}/temp/"
)
job.commit()