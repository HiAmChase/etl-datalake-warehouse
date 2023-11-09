from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Transform") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    execute_date = spark.conf.get("spark.execute.date")

    if not execute_date:
        raise Exception("You should define the EXECUTE_DATE")

    year, month, day = execute_date.split("-")
    datalake_path = "hdfs://localhost:9000/datalake"

    order_df = spark.read.parquet(f"{datalake_path}/order") \
        .drop("year", "month", "day")
    order_detail_df = spark.read.parquet(f"{datalake_path}/order-detail") \
        .drop("year", "month", "day")
    product_df = spark.read.parquet(f"{datalake_path}/product") \
        .drop("year", "month", "day")

    pre_df = order_df \
        .filter(col("order_date") == execute_date) \
        .join(order_detail_df, order_df.id == order_detail_df.order_id, "inner") \
        .select(order_detail_df["product_id"], order_detail_df["quantity"])

    map_df = pre_df.groupBy("product_id").agg(sum("quantity").alias("Sales"))

    result_df = map_df \
        .join(product_df, map_df.product_id == product_df.id, "inner") \
        .withColumn("Revenue", product_df["price"] * col("Sales")) \
        .withColumn("year", lit(year)) \
        .withColumn("month", lit(month)) \
        .withColumn("day", lit(day)) \
        .withColumnRenamed("name", "Name") \
        .withColumnRenamed("category", "Category") \
        .select("Name", "Category", "Sales", "Revenue", "year", "month", "day")

    result_df.show()

    result_df.write \
        .format("hive") \
        .partitionBy("year", "month", "day") \
        .mode("append") \
        .saveAsTable("reports.daily_gross_revenue")

    spark.stop()


if __name__ == "__main__":
    main()
