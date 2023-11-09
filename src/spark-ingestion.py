from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql.functions import *
import config


def main():
    spark = SparkSession.builder.appName("Ingestion").getOrCreate()

    java_import(spark._jvm, "org.apache.hadoop.fs.Path")
    java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
    java_import(spark._jvm, "org.apache.hadoop.fs.FSDataInputStream")

    execute_date = spark.conf.get("spark.execute.date")
    table_name = spark.conf.get("spark.postgres.table")

    if not execute_date:
        raise Exception("You should define the EXECUTE_DATE")

    year, month, day = execute_date.split("-")
    hdfs_path = f"hdfs://localhost:9000/datalake/{table_name}"
    hadoop_conf = spark._jsc.hadoopConfiguration()

    path = spark._jvm.Path(hdfs_path)
    fs = path.getFileSystem(hadoop_conf)
    path_exists = fs.exists(path)

    if path_exists:
        df = spark.read.parquet(hdfs_path)
        max_record_id = df.agg(max("id")).collect()[0][0]
        tbl_query = f'(select * from "{table_name}" where id > {max_record_id}) tmp'
    else:
        tbl_query = f'(select * from "{table_name}") as tmp'

    jdbc_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{config.PSQL_HOST}:{config.PSQL_PORT}/{config.PSQL_DB}") \
        .option("dbtable", tbl_query) \
        .option("user", config.PSQL_USER) \
        .option("password", config.PSQL_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    output_df = jdbc_df.withColumn("year", lit(year)) \
        .withColumn("month", lit(month)) \
        .withColumn("day", lit(day))

    partition = ["year", "month", "day"]
    output_df.write.mode("append").parquet(hdfs_path, partitionBy=partition)

    spark.stop()


if __name__ == "__main__":
    main()
