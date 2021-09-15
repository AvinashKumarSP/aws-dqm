import sys

import pyspark.sql.functions
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField

from dynamodb_utils import get_rule
from lib.logger import Log4j
from lib.utils import *
import pydeequ
from pydeequ.suggestions import *


def gt(n):
    return col("pass_percentage") > n


def lt(n):
    return col("pass_percentage") < n


def eq(n):
    return col("pass_percentage") == n


def ge(n):
    return col("pass_percentage") >= n


def le(n):
    return col("pass_percentage") <= n


dq_rule = {
    "source": "nhs",
    "entity": "patient",
    "cde": ["patient_id", "age"],
    "kde": ["country", "state"],
    "rules_run_on": ["cde", "kde"],
    "primary_key": "patient_id",
    "single_column_validation_rules": {
        "patient_id": [
            {
                "rule_specification": "Null Check",
                "rule_name": "patient_id_not_null",
                "rule": "patient_id is not null",
                "dimension": "completeness",
                "is_active": 1,
                "slo": "gt(90)"
            }
        ],
        "country": [
            {
                "rule_specification": "Conditional Check",
                "rule_name": "country_has",
                "rule": "country in ('United States','United Kingdom')",
                "dimension": "conformity",
                "is_active": 1,
                "slo": "gt(90)"
            },
            {
                "rule_specification": "Null Check",
                "rule_name": "country_not_null",
                "rule": "country is not null",
                "dimension": "completeness",
                "is_active": 1,
                "slo": "gt(90)"
            }
        ]
    }
}

# dq_rule = get_rule("nhs", "patient")
rule_elements = []
if "cde" in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde'] + dq_rule['kde']
elif "cde" in dq_rule['rules_run_on'] and "kde" not in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde']
elif "cde" not in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['kde']
rules = dq_rule['single_column_validation_rules']

if __name__ == "__main__":
    conf = get_spark_app_config()
    print(rules)
    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)
    partitioned_survey_df.show()
    rule = "isnull(patient_id)"
    test_list = ["patient_id", "country"]
    rule_df = partitioned_survey_df.select(test_list).withColumn("length_of_country_id_gt_0",
                                                                 expr("Country in('United States','United Kingdom')"))

    results_df = spark.createDataFrame([], StructType([]))
    metrics_df = spark.createDataFrame([], StructType([]))
    count = 0
    for element, rules_list in rules.items():
        if element in rule_elements:
            for rule in rules_list:
                if rule["is_active"] == 1:
                    current_df = partitioned_survey_df.select(test_list).withColumn("rule_applied_on", lit(element)) \
                        .withColumn("source", lit(dq_rule['source'])) \
                        .withColumn("entity", lit(dq_rule['entity'])) \
                        .withColumn("rule", lit(rule['rule'])) \
                        .withColumn("rule_name", lit(rule['rule_name'])) \
                        .withColumn("dimension", lit(rule['dimension'])) \
                        .withColumn("value", partitioned_survey_df[element]) \
                        .withColumn("result", expr(rule['rule'])) \
                        .withColumn("run_date", current_timestamp())
                    current_metrics_df = current_df.select('source', 'entity', 'rule_applied_on', 'rule', 'rule_name',
                                                           'dimension', 'result') \
                        .withColumn('success_record', when(col('result') == 'true', 1).otherwise(0)) \
                        .withColumn('failure_record', when(col('result') == 'false', 1).otherwise(0)) \
                        .groupBy('source', 'entity', 'rule_applied_on', 'rule', 'rule_name', 'dimension') \
                        .agg(sum(col('success_record')).alias("success_count"),
                             sum(col('failure_record')).alias("failure_count"),
                             pyspark.sql.functions.count('*').alias('count')) \
                        .withColumn("pass_percentage", round(col("success_count") / col("count") * 100, 2)) \
                        .withColumn("constraint_status", eval(rule['slo']))
                    if count == 0:
                        results_df = current_df
                        metrics_df = current_metrics_df
                        count += 1
                    else:
                        results_df = results_df.union(current_df)
                        metrics_df = metrics_df.union(current_metrics_df)

    results_df.show(100, truncate=False)
    metrics_df.show(truncate=False)

    logger.info("Finished HelloSpark")
    spark.stop()
