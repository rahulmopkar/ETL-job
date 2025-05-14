import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, date_format, dayofweek, dayofmonth, dayofyear, weekofyear,
    year, month, quarter, when, concat, lpad, last_day
)
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define start and end dates
start_date = datetime.date(2010, 1, 1)
end_date = datetime.date(2025, 12, 31)
date_list = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]
date_df = spark.createDataFrame([(d,) for d in date_list], ["date"])

# Fiscal start month (e.g., July = 7)
fiscal_start_month = 7

# Create calendar-based columns
df = (
    date_df
    .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("full_date_desc", date_format("date", "EEEE, MMMM d, yyyy"))
    .withColumn("day_of_week", date_format("date", "E"))
    .withColumn("day_number_in_month", dayofmonth("date"))
    .withColumn("day_number_in_year", dayofyear("date"))
    .withColumn("calendar_week", weekofyear("date"))
    .withColumn("calendar_month", month("date"))
    .withColumn("calendar_month_name", date_format("date", "MMMM"))
    .withColumn("calendar_year", year("date"))
    .withColumn("calendar_quarter", quarter("date"))
    .withColumn("calendar_year_month", date_format("date", "yyyy-MM"))
    .withColumn("calendar_year_quarter", concat(year("date"), lit(" Q"), quarter("date")))
    .withColumn("last_day_in_month", (date_df["date"] == last_day(date_df["date"])))
    .withColumn("weekday_indicator", (dayofweek("date") < 6).cast("int"))
    .withColumn("holiday_indicator", lit(0))  # Placeholder
)

# Add fiscal columns
df = (
    df
    .withColumn("fiscal_month_number_in_year", when(month("date") >= fiscal_start_month,
                                                    month("date") - fiscal_start_month + 1)
                .otherwise(month("date") + (12 - fiscal_start_month + 1)))
    .withColumn("fiscal_year", when(month("date") >= fiscal_start_month, year("date") + 1).otherwise(year("date")))
    .withColumn("fiscal_month", date_format("date", "MMMM"))
    .withColumn("fiscal_year_month", concat(col("fiscal_year"), lit("-"), lpad(col("fiscal_month_number_in_year").cast("string"), 2, "0")))
    .withColumn("fiscal_quarter", (((col("fiscal_month_number_in_year") - 1) / 3).cast("int") + 1))
    .withColumn("fiscal_year_quarter", concat(col("fiscal_year"), lit(" Q"), col("fiscal_quarter")))
    .withColumn("fiscal_half_year", when(col("fiscal_month_number_in_year") <= 6, lit("H1")).otherwise(lit("H2")))
    .withColumn("fiscal_week_number_in_year", weekofyear("date"))  # Approximate, customize if needed
)

# Save to S3 in csv format
df.write.mode("overwrite").csv("s3://practice-bucket-glue-spark/retail_data_model/dimensions/dim_date/")

job.commit()
