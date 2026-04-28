from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, upper, unix_timestamp, datediff, round, current_timestamp, current_date
from pyspark.sql import SparkSession
import traceback
from pyspark.sql.utils import AnalysisException
import argparse

# ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

CUSTOMERS = "/customers/"
ITEMS = "/items/"
PAYMENTS = "/payments/"
ORDERS = "/orders/"
PRODUCTS = "/products/"
SELLERS = "/sellers/"


try:

    # SPARK
    spark = SparkSession.builder.appName("SilverJob").getOrCreate()
    print("SparkSession initialized for SilverJob")

    # READ INPUT
    # Using .csv(args.input) is fine, but ensure the path is correct

    # CUSTOMERS Data --------------------------------------------------
    customers_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), False),
        StructField("customer_zip_code_prefix", StringType(), False),
        StructField("customer_city", StringType(), False),
        StructField("customer_state", StringType(), False)
    ])

    customers_df = spark.read.format("csv").options(header=True).schema(customers_schema).load(args.input+CUSTOMERS)

    customers_df = customers_df.withColumn("customer_city", upper(col("customer_city"))) \
        .withColumn("customer_state", upper(col("customer_state")))

    # ITEMS Data ------------------------------------------------------
    items_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("shipping_limit_date", TimestampType(), False),
        StructField("price", DoubleType(), False),
        StructField("freight_value", DoubleType(), False)
    ])

    items_df = spark.read \
        .format("csv") \
        .options(header=True, timestampFormat="yyyy-MM-dd HH:mm:ss") \
        .schema(items_schema) \
        .load(args.input+ITEMS)

    items_df = items_df.withColumn("total_cost", col("price")+col("freight_value"))


    # PAYMENTS Data -------------------------------------------
    payments_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), False),
        StructField("payment_type", StringType(), False),
        StructField("payment_installments", IntegerType(), False),
        StructField("payment_value", DoubleType(), False)
    ])

    payments_df = spark.read \
        .format("csv") \
        .options(header=True) \
        .schema(payments_schema) \
        .load(args.input+PAYMENTS)

    payments_df = payments_df.withColumn("payment_type", upper(col("payment_type")))



    # ORDERS Data ----------------------------------------------
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("order_purchase_timestamp", TimestampType(), False),
        StructField("order_approved_at", TimestampType(), False),
        StructField("order_delivered_carrier_date", TimestampType(), False),
        StructField("order_delivered_customer_date", TimestampType(), False),
        StructField("order_estimated_delivery_date", TimestampType(), False)
    ])

    orders_df = spark.read \
        .format("csv") \
        .options(header=True, timestampFormat="yyyy-MM-dd HH:mm:ss") \
        .schema(orders_schema) \
        .load(args.input+ORDERS)



    orders_df = orders_df.withColumn("order_status", upper(col("order_status")))

    # delivery_time is only applicable for delivered orders   
    orders_df = orders_df.withColumn("actual_delivery_hours", 
        (round((unix_timestamp("order_delivered_customer_date") - unix_timestamp("order_delivered_carrier_date")) / 3600, 2).cast("double"))) \
        .withColumn("estimated_delivery_hours", 
        (round((unix_timestamp("order_estimated_delivery_date") - unix_timestamp("order_delivered_carrier_date")) / 3600, 2).cast("double"))) \
        .withColumn("is_delayed", (datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")) > 0).cast("boolean"))


    # PRODUCTS Data --------------------------------
    products_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType(), False),
        StructField("product_name_lenght", IntegerType(), False),
        StructField("product_description_lenght", IntegerType(), False),
        StructField("product_photos_qty", IntegerType(), False),
        StructField("product_weight_g", IntegerType(), False),
        StructField("product_length_cm", IntegerType(), False),
        StructField("product_height_cm", IntegerType(), False),
        StructField("product_width_cm", IntegerType(), False)
    ])

    products_df = spark.read \
        .format("csv") \
        .options(header=True) \
        .schema(products_schema) \
        .load(args.input+PRODUCTS)

    # cleaning
    products_df = products_df.withColumn("product_category_name", upper(col("product_category_name")))
    products_df = products_df.fillna({
        "product_category_name": "OTHERS"
    })

    # transformations
    products_df = products_df.withColumn("product_volume_cm3", (col("product_length_cm") * col("product_height_cm") * col("product_width_cm")).cast("double"))


    # SELLERS Data ------------------------------------------------------
    sellers_schema = StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", StringType(), False),
        StructField("seller_city", StringType(), False),
        StructField("seller_state", StringType(), False)
    ])

    sellers_df = spark.read \
        .format("csv") \
        .options(header=True) \
        .schema(sellers_schema) \
        .load(args.input+SELLERS)

    sellers_df = sellers_df.withColumn("seller_city", upper(col("seller_city"))) \
        .withColumn("seller_state", upper(col("seller_state")))

    print("Transformations are completed. Writing data to the silver folder")


    # Adding tranformated timestamp column to all the datasets
    customers_df = customers_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())
    items_df = items_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())
    orders_df = orders_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())
    payments_df = payments_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())
    products_df = products_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())
    sellers_df = sellers_df.withColumn("transformation_timestamp", current_timestamp()).withColumn("transformation_date", current_date())


    # Writing data to silver folder
    customers_df.write.mode("append").parquet(args.output+CUSTOMERS)
    items_df.write.mode("append").parquet(args.output+ITEMS)
    orders_df.write.mode("append").parquet(args.output+ORDERS)
    payments_df.write.mode("append").parquet(args.output+PAYMENTS)
    products_df.write.mode("append").parquet(args.output+PRODUCTS)
    sellers_df.write.mode("append").parquet(args.output+SELLERS)

    print("Silver Layer processing complete.")

except AnalysisException as ae:
    print("❌ Spark Analysis Error (schema/column issue)")
    print(str(ae))
    raise

except Exception as e:
    print("❌ Unexpected Error")
    print(traceback.format_exc())
    raise
