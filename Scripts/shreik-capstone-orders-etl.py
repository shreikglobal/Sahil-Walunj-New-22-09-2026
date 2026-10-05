import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


source_path = "s3://shreik-capstone-sahil-2026/raw/orders/raw_orders.csv"
warehouse_path = "s3://shreik-capstone-sahil-2026/curated/orders/"


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)


df = (
    df
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("discount_pct", col("discount_pct").cast("double"))
    .withColumn("gross_amount", col("gross_amount").cast("double"))
    .withColumn("discount_amount", col("discount_amount").cast("double"))
    .withColumn("net_amount", col("net_amount").cast("double"))
    .withColumn("shipping_days", col("shipping_days").cast("int"))
    .withColumn("order_date", to_date(col("order_date")))
)


clean_df = df.filter(
    col("customer_id").isNotNull()
    & (col("quantity") > 0)
    & (col("unit_price") > 0)
    & (col("region").isin("North", "South", "East", "West", "Central"))
    & (col("status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED"))
)


clean_df = clean_df.dropDuplicates(["order_id"])

spark.conf.set(
    "spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    warehouse_path
)


spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.shreik_capstone_db.curated_orders (
    order_id string,
    order_ts string,
    customer_id string,
    product_id string,
    category string,
    region string,
    quantity int,
    unit_price double,
    discount_pct double,
    payment_method string,
    status string,
    shipping_days int,
    gross_amount double,
    discount_amount double,
    net_amount double,
    order_date date
)
USING iceberg
PARTITIONED BY (days(order_date))
LOCATION 's3://shreik-capstone-sahil-2026/curated/orders/'
""")


clean_df.createOrReplaceTempView("clean_orders")

spark.sql("""
INSERT OVERWRITE glue_catalog.shreik_capstone_db.curated_orders
SELECT
    order_id,
    order_ts,
    customer_id,
    product_id,
    category,
    region,
    quantity,
    unit_price,
    discount_pct,
    payment_method,
    status,
    shipping_days,
    gross_amount,
    discount_amount,
    net_amount,
    order_date
FROM clean_orders
""")

job.commit()
