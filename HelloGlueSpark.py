import sys

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField

from lib.logger import Log4j
from lib.utils import *
import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.suggestions import *


def gt(n):
    return lambda x: x > n


def lt(n):
    return lambda x: x < n


def eq(n):
    return lambda x: x == n


def ge(n):
    return lambda x: x >= n


def le(n):
    return lambda x: x <= n


if __name__ == "__main__":
    conf = get_spark_app_config()

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
    test_list = ["patient_id", "Country"]
    rule_df = partitioned_survey_df.select(test_list).withColumn("length_of_country_id_gt_0",
                                                                 expr("Country in('United States','United Kingdom')"))
    rule_df.show()

    new_req_rules_df = partitioned_survey_df.select(test_list)
    deequ_profiler_dict = {"completeness": Completeness,
                           "distinctness": Distinctness,
                           "max_length": MaxLength,
                           "min_length": MinLength,
                           "uniqueness": Uniqueness}
    rules = {
        "profiler_rules": [
            {
                "rule_name": "completeness",
                "on_columns": ["patient_id", "age", "country", "state"]
            },
            {
                "rule_name": "distinctness",
                "on_columns": ["patient_id", "age", "country", "state"]
            }
        ]
    }
    rules_list = rules['profiler_rules']
    rules_list1 = {c['rule_name']: c['on_columns'] for c in rules_list}
    analysisResult1 = AnalysisRunner(spark) \
        .onData(partitioned_survey_df)
    for key, values in rules_list1.items():
        #print(key, values)
        deequ_func = deequ_profiler_dict[key]
        #print(deequ_func)
        for column in values:
            analysisResult1.addAnalyzer(deequ_func(column))

    analysisResult2 = analysisResult1.run()
    analysisResult1_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult2)
    #analysisResult1_df.show()

    completeness = Completeness
    analysisResult = AnalysisRunner(spark) \
        .onData(partitioned_survey_df) \
        .addAnalyzer(Size()) \
        .addAnalyzer(eval("Distinctness")("patient_id")) \
        .addAnalyzer(completeness("patient_id")) \
        .addAnalyzer(Uniqueness(["patient_id"])) \
        .addAnalyzer(Compliance("country_validity", "country in ('United States','United Kingdom')")) \
        .addAnalyzer(Compliance("patient_id_completeness", "patient_id is not null")) \
        .run()

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)\
        .withColumn("db_entity", lit("patient_data"))\
        .withColumn("source", lit("nhs"))
    analysisResult_df.show()

    suggestionResult = ConstraintSuggestionRunner(spark) \
        .onData(partitioned_survey_df) \
        .addConstraintRule(DEFAULT()) \
        .run()
    # Constraint Suggestions in JSON format
    from pydeequ.checks import *
    from pydeequ.verification import *

    check = Check(spark, CheckLevel.Error, "Patient Data Check")
    check1 = Check(spark, CheckLevel.Warning, "Patient Data Check1")

    checkResult = VerificationSuite(spark) \
        .onData(partitioned_survey_df) \
        .addCheck(
        check.__getattribute__("hasSize")(lambda x: x >= 15, "Failed due to more than expected record count")
            .__getattribute__("isComplete")("patient_id")
            .hasSize(ge(8))
            .isUnique("patient_id")
            .isComplete("country")
            .__getattribute__("hasCompleteness")('patient_id', eval("gt(0.9)"))
            .satisfies("country in ('United States','United Kingdom')", "country_has", lambda x: x >= 0.9, None)
            .isContainedIn("country", ["United States", "United Kingdom"])) \
        .addCheck(check1.isContainedIn("country", ["United States", "United Kingdom"])) \
        .run()

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df1 = checkResult_df.withColumn('source', lit('nhs')).withColumn('entity', lit('patient_data'))
    checkResult_df1.show(truncate=False)

    logger.info("Finished HelloSpark")
    spark.stop()
