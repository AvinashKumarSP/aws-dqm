import sys


from pydeequ.analyzers import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField

from dynamodb_utils import get_rule
from lib.logger import Log4j
from lib.utils import *
import pydeequ
from pydeequ.suggestions import *


dq_rule = get_rule("nhs", "patient")
rule_elements = []
if "cde" in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde'] + dq_rule['kde']
elif "cde" in dq_rule['rules_run_on'] and "kde" not in dq_rule['rules_run_on']:
    rule_elements = dq_rule['cde']
elif "cde" not in dq_rule['rules_run_on'] and "kde" in dq_rule['rules_run_on']:
    rule_elements = dq_rule['kde']
rules = dq_rule['single_column_profiler_rule']

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

    analysisResult = AnalysisRunner(spark) \
        .onData(partitioned_survey_df) \
        .addAnalyzer(Size()) \
        .addAnalyzer(Distinctness("patient_id")) \
        .addAnalyzer(completeness("patient_id")) \
        .addAnalyzer(Uniqueness(["patient_id"])) \
        .addAnalyzer(Compliance("country_validity", "country in ('United States','United Kingdom')")) \
        .addAnalyzer(Compliance("patient_id_completeness", "patient_id is not null")) \
        .run()

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult).withColumn("table",
                                                                                                    lit("patient_data"))
    analysisResult_df.show()
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

    logger.info("Finished HelloSpark")
    spark.stop()
