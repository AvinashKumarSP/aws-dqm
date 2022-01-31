import sys
from pyspark.sql import *
from pyspark.sql.functions import col, lit, current_timestamp, year, dayofmonth, month
from pyspark.sql.types import StructType
import json

from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .appName("SparkTransformation") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 3:
        logger.error("Usage: HelloSpark <source_file> <ext_file>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    json_mapping = {
        "src": "sse",
        "entity": "sse_account",
        "external_entity": "external_ref",
        "source_primary_key": "sse_account_id",
        "primary_key_mapping": "business_regn_no=company_no",
        "src_ext_mapping": [
            "business_name=business_name",
            "business_regn_dt=registration_dt",
            "business_regn_country=registered_country"
        ]
    }

    source_df = load_df(spark, sys.argv[1])
    ext_df = load_df(spark, sys.argv[2])
    source_df.show()
    ext_df.show()
    src_cols = [col(value).alias("src_" + value) for value in source_df.columns]
    source_new_df = source_df.select(src_cols)
    ext_cols = [col(value).alias("ext_" + value) for value in ext_df.columns]
    ext_new_df = ext_df.select(ext_cols)
    source_new_df.show()
    ext_new_df.show()
    # joined_df = source_df.join(ext_df, source_df.business_regn_no == ext_df.company_no, "outer")
    external_entity = json_mapping["external_entity"]
    source_primary_key = json_mapping["source_primary_key"]
    source_new_primary_key = "src_" + source_primary_key
    source_join_key = json_mapping["primary_key_mapping"].split("=")[0]
    source_new_join_key = "src_" + source_join_key
    external_join_key = json_mapping["primary_key_mapping"].split("=")[1]
    external_new_join_key = "ext_" + external_join_key
    publish_date_column = "ext_" + "publish_date"
    joined_df = source_new_df.join(ext_new_df, source_new_df[source_new_join_key] == ext_new_df[external_new_join_key],
                                   "outer")
    joined_df.show()
    mapping_list = json_mapping["src_ext_mapping"]
    results_df = spark.createDataFrame([], StructType([]))
    count = 0
    for item in mapping_list:
        source_col_name = item.split("=")[0]
        source_new_col_name = "src_" + source_col_name
        external_col_name = item.split("=")[1]
        external_new_col_name = "ext_" + external_col_name
        joined_df = joined_df \
            .withColumn("primary_key", lit(source_primary_key)) \
            .withColumn("primary_key_value", joined_df[source_new_primary_key]) \
            .withColumn("source_col_name", lit(source_col_name)) \
            .withColumn("source_col_value", joined_df[source_new_col_name]) \
            .withColumn("external_col_name", lit(source_col_name)) \
            .withColumn("external_col_value", joined_df[external_new_col_name]) \
            .withColumn("external_data_source", lit(external_entity)) \
            .withColumn("external_data_publish_date", joined_df[publish_date_column]) \
            .withColumn("run_date", current_timestamp()) \
            .withColumn("year", year("run_date")) \
            .withColumn("month", month("run_date")) \
            .withColumn("day", dayofmonth("run_date"))
        # .select(source_primary_key, source_join_key, external_join_key)
        selected_df = joined_df.selectExpr("primary_key", "primary_key_value", "source_col_name as attribute_name",
                                           "source_col_value as current_value", "external_col_value as expected_value",
                                           "external_data_source", "external_data_publish_date", "run_date", "year",
                                           "month", "day")
        if count == 0:
            results_df = selected_df
            count += 1
        else:
            results_df = results_df.union(selected_df)

    results_df.show()
    logger.info("Finished HelloSpark")
    spark.stop()
