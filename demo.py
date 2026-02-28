"""
Simple PySpark ETL example (extract -> transform -> load).

This script demonstrates a minimal, runnable ETL pipeline that:
- creates sample input data if none is provided
- reads CSV input (or uses in-memory sample)
- performs simple transformations and aggregation
- writes output as Parquet and CSV

Run with: `python demo.py` or `spark-submit demo.py`
"""

import argparse
import os
import tempfile
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


def create_sample_csv(path):
	"""Write small sample CSV files for demo purposes."""
	spark = SparkSession.builder.getOrCreate()
	customers = [
		(1, "Alice", "NY"),
		(2, "Bob", "CA"),
		(3, "Cathy", "WA"),
	]
	transactions = [
		(101, 1, 23.5),
		(102, 1, 15.0),
		(103, 2, 99.99),
		(104, 3, 8.5),
		(105, 2, 20.0),
	]

	cust_schema = T.StructType([
		T.StructField("customer_id", T.IntegerType(), False),
		T.StructField("name", T.StringType(), False),
		T.StructField("state", T.StringType(), True),
	])

	txn_schema = T.StructType([
		T.StructField("tx_id", T.IntegerType(), False),
		T.StructField("customer_id", T.IntegerType(), False),
		T.StructField("amount", T.DoubleType(), False),
	])

	cust_df = spark.createDataFrame(customers, schema=cust_schema)
	txn_df = spark.createDataFrame(transactions, schema=txn_schema)

	cust_dir = os.path.join(path, "customers")
	txn_dir = os.path.join(path, "transactions")
	cust_df.coalesce(1).write.mode("overwrite").option("header", True).csv(cust_dir)
	txn_df.coalesce(1).write.mode("overwrite").option("header", True).csv(txn_dir)


def build_spark(app_name="pyspark-etl-demo"):
	return SparkSession.builder.appName(app_name).getOrCreate()


def run_etl(spark, input_base, output_base):
	# Read inputs
	cust_path = os.path.join(input_base, "customers")
	txn_path = os.path.join(input_base, "transactions")

	cust_df = spark.read.option("header", True).option("inferSchema", True).csv(cust_path)
	txn_df = spark.read.option("header", True).option("inferSchema", True).csv(txn_path)

	# Basic transform: compute total spend per customer
	agg = (
		txn_df.groupBy("customer_id")
		.agg(F.count("tx_id").alias("tx_count"), F.sum("amount").alias("total_amount"))
	)

	result = (
		agg.join(cust_df, agg.customer_id == cust_df.customer_id, how="left")
		.select(
			cust_df.customer_id,
			cust_df.name,
			cust_df.state,
			agg.tx_count,
			F.round(agg.total_amount, 2).alias("total_amount"),
		)
		.orderBy(F.desc("total_amount"))
	)

	# Show result
	result.show(truncate=False)

	# Write outputs
	parquet_out = os.path.join(output_base, "customers_total.parquet")
	csv_out = os.path.join(output_base, "customers_total_csv")
	result.write.mode("overwrite").parquet(parquet_out)
	result.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_out)


def parse_args():
	p = argparse.ArgumentParser(description="Simple PySpark ETL demo")
	p.add_argument("--input", help="Input base folder (contains customers/ and transactions/)", default=None)
	p.add_argument("--output", help="Output folder", default="output")
	p.add_argument("--generate-sample", action="store_true", help="Generate sample input data into input folder")
	return p.parse_args()


def main():
	args = parse_args()
	spark = build_spark()

	# If no input provided, create a temp folder with sample CSVs
	if args.input is None:
		tmp = tempfile.mkdtemp(prefix="pyspark_etl_")
		print(f"No input specified — generating sample data in {tmp}")
		create_sample_csv(tmp)
		input_base = tmp
	else:
		input_base = args.input
		if args.generate_sample:
			os.makedirs(input_base, exist_ok=True)
			create_sample_csv(input_base)

	os.makedirs(args.output, exist_ok=True)
	run_etl(spark, input_base, args.output)
	spark.stop()


if __name__ == "__main__":
	main()

