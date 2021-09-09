import sys

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField

from dynamodb_utils import get_rule
from lib.logger import Log4j
from lib.utils import *
import pydeequ
from pydeequ.suggestions import *

'''
rules = {
    "patient_id": [
        {
            "rule_specification": "patient_id has null",
            "rule_name": "patient_id_isnull",
            "rule": "patient_id is not null",
        }
    ],
    "country": [
        {
            "rule_specification": "Country has ['United States','United Kingdom']",
            "rule_name": "country_has",
            "rule": "country in ('United States','United Kingdom')"
        },
        {
            "rule_specification": "Country is not null",
            "rule_name": "country_has",
            "rule": "country is not null"
        }
    ]
}
'''

dq_rule = get_rule("nhs", "patient")
rule_elements = []
if "cde" in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde'] + dq_rule['kde']
elif "cde" in dq_rule['rules_run_on'] and "kde" not in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde']
elif "cde" not in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['kde']
rules = dq_rule['single_column_validation_rule']

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

    count = 0
    for element, rules_list in rules.items():
        if element in rule_elements:
            for rule in rules_list:
                if rule["is_active"] == 1:
                    current_df = partitioned_survey_df.select(test_list).withColumn("rule_applied_on", lit(element)) \
                        .withColumn("rule", lit(rule['rule'])) \
                        .withColumn("value", partitioned_survey_df[element]) \
                        .withColumn("result", expr(rule['rule'])) \
                        .withColumn("run_date", current_timestamp())
                    if count == 0:
                        results_df = current_df
                        count += 1
                    else:
                        results_df = results_df.union(current_df)

    results_df.explain()
    results_df.show(100, truncate=False)

    logger.info("Finished HelloSpark")
    spark.stop()
