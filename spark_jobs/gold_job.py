from pyspark.sql.functions import col, date_trunc, sum, count, date_format, round, datediff, countDistinct, max, lit, avg
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
    spark = SparkSession.builder.appName("GoldJob").getOrCreate()
    print("SparkSession initialized for GoldJob")

    ## READ INPUT ----------------------------------------------

    # CUSTOMERS Data -------
    customers_df = spark.read.format("parquet").options(header=True, inferSchema=True).load(args.input+CUSTOMERS)

    # ITEMS Data ----------
    items_df = spark.read \
        .format("parquet") \
        .options(header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss") \
        .load(args.input+ITEMS)

    # PAYMENTS Data ---------
    payments_df = spark.read \
        .format("parquet") \
        .options(header=True, inferSchema=True) \
        .load(args.input+PAYMENTS)


    # ORDERS Data --------
    orders_df = spark.read \
        .format("parquet") \
        .options(header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss") \
        .load(args.input+ORDERS)

    # PRODUCTS Data -----------
    products_df = spark.read \
        .format("parquet") \
        .options(header=True, inferSchema=True) \
        .load(args.input+PRODUCTS)

    # SELLERS Data ------
    sellers_df = spark.read \
        .format("parquet") \
        .options(header=True, inferSchema=True) \
        .load(args.input+SELLERS)


    print("Reading the data from silver layer is done. Now starting the aggregations")

    ## AGGREGATIONS------------------------------------------------------------------------

    # A. Revenue & Sales Performance (Monthly Category Trends)
    # ---------------------------------------------------------
    revenue_df = items_df.join(orders_df, "order_id") \
        .join(products_df, "product_id") \
        .withColumn("order_month", date_format(date_trunc("month", col("order_purchase_timestamp")), "yyyy-MM")) \
        .groupBy("order_month", col("product_category_name")) \
        .agg(
            round(sum("price"), 3).alias("monthly_revenue"),
            count("order_id").alias("order_count")
        ) \
        .orderBy("order_month", col("monthly_revenue").desc(), col("order_count").desc())


    # B. Customer RFM Analysis (Recency, Frequency, Monetary)
    # Recency: Days since last purchase
    # Frequency: Number of unique orders
    # Monetary: Total spend
    current_date = orders_df.select(max("order_purchase_timestamp")).collect()[0][0]
    rfm_df = orders_df.join(payments_df, "order_id") \
        .join(customers_df, "customer_id") \
        .groupBy("customer_unique_id") \
        .agg(
            datediff(lit(current_date), max("order_purchase_timestamp")).alias("recency"),
            countDistinct("order_id").alias("frequency"),
            sum("payment_value").alias("monetary")
        )

    # C. Logistics & Seller Performance
    # Assessing delivery efficiency by region
    logistics_df = orders_df.join(items_df, "order_id") \
        .join(sellers_df, "seller_id") \
        .filter(col("order_status") == "DELIVERED") \
        .groupBy("seller_id", "seller_state", "seller_city") \
        .agg(
            avg("actual_delivery_hours").alias("avg_delivery_hours"),
            avg("freight_value").alias("avg_shipping_cost"),
            count("order_id").alias("total_items_sold")
        )

    # D. Payment Methods Summary
    payment_summary_df = payments_df.groupBy("payment_type") \
        .agg(
            sum("payment_value").alias("total_payment_value"),
            avg("payment_installments").alias("avg_installments"),
            count("order_id").alias("transaction_count")
        )

    revenue_df.write.mode("overwrite").parquet(f"{args.output}/fact_revenue/")
    rfm_df.write.mode("overwrite").parquet(f"{args.output}/dim_customer_rfm/")
    logistics_df.write.mode("overwrite").parquet(f"{args.output}/fact_logistics/")
    payment_summary_df.write.mode("overwrite").parquet(f"{args.output}/dim_payments/")

    print("Gold Layer processing complete.")
except AnalysisException as ae:
    print("❌ Spark Analysis Error (schema/column issue)")
    print(str(ae))
    raise
except Exception as e:
    print("❌ Unexpected Error")
    print(traceback.format_exc())
    raise
