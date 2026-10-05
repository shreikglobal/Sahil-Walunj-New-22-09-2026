import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F



args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)



spark.conf.set(
    "spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    "s3://shreik-capstone-sahil-2026/curated/"
)



raw_path = "s3://shreik-capstone-sahil-2026/raw/orders/"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)



df = (
    df
    .withColumn("order_date", F.to_date("order_ts"))
    .dropDuplicates(["order_id"])
)


spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.shreik_capstone_db
""")



spark.sql("""
CREATE TABLE IF NOT EXISTS
glue_catalog.shreik_capstone_db.curated_orders
USING iceberg
PARTITIONED BY (days(order_date))
LOCATION 's3://shreik-capstone-sahil-2026/curated/orders/'
AS
SELECT *
FROM df
WHERE 1 = 0
""")



df.createOrReplaceTempView("orders_source")

spark.sql("""
INSERT OVERWRITE glue_catalog.shreik_capstone_db.curated_orders
SELECT *
FROM orders_source
""")



job.commit()
