import sys
from pyspark.sql import *
from pyspark.sql.types import StructType, StringType, StructField

from lib.logger import Log4j
from lib.utils import *
import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *

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

    analysisResult = AnalysisRunner(spark) \
        .onData(partitioned_survey_df) \
        .addAnalyzer(Completeness("patient_id")) \
        .addAnalyzer(ApproxCountDistinct("patient_id")) \
        .run()

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
    analysisResult_df.show()

    result = ColumnProfilerRunner(spark) \
        .onData(partitioned_survey_df) \
        .run()
    for col, profile in result.profiles.items():
        print(profile)

    logger.info("Finished HelloSpark")
    spark.stop()
