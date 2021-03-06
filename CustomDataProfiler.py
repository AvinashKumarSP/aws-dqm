import sys

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
import time

from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import load_survey_df, get_spark_app_config
from pyspark.sql.types import *


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]':
        return TimestampType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except Exception as ex:
        typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)


def dataprofile(data_all_df, data_cols):
    data_df = data_all_df.select(data_cols)
    columns2Bprofiled = data_df.columns
    global schema_name, table_name
    if not 'schema_name' in globals():
        schema_name = 'schema_name'
    if not 'table_name' in globals():
        table_name = 'table_name'
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    dprof_df = pd.DataFrame({'schema_name': [schema_name] * len(data_df.columns), \
                             'table_name': [table_name] * len(data_df.columns), \
                             'column_names': data_df.columns, \
                             'data_types': [x[1] for x in data_df.dtypes]})
    dprof_df = dprof_df[['schema_name', 'table_name', 'column_names', 'data_types']]
    dprof_df.set_index('column_names', inplace=True, drop=True)
    # ======================
    num_rows = data_df.count()
    dprof_df['num_rows'] = num_rows
    # ======================
    # number of rows with nulls and nans
    df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns \
                                  if data_df.select(c).dtypes[0][1] != 'timestamp']).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['column_names', 'num_null']
    print("print 1", dprof_df)
    print("print 2", df_nacounts)
    dprof_df = pd.merge(dprof_df, df_nacounts, on=["column_names"], how='left')
    print("print 3", dprof_df)
    # ========================
    # number of rows with white spaces (one or more space) or blanks
    num_spaces = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
    dprof_df['num_spaces'] = num_spaces
    num_blank = [data_df.where(F.col(c) == '').count() for c in data_df.columns]
    dprof_df['num_blank'] = num_blank
    # =========================
    # using the in built describe() function
    print("print 4", dprof_df)
    print("print 5", data_df.describe().toPandas())
    desc_df = data_df.describe().toPandas().transpose()
    print("print 6", desc_df)
    desc_df.columns = ['count', 'mean', 'stddev', 'min', 'max']
    desc_df = desc_df.iloc[1:, :]
    print("print 7", desc_df)
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'column_names'
    desc_df = desc_df[['column_names', 'count', 'mean', 'stddev']]
    dprof_df = pd.merge(dprof_df, desc_df, on=['column_names'], how='left')
    print("print 8", dprof_df)
    # ===========================================
    allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    # allmincounts = [data_df.where(col(x) == y).count() for x, y in zip(columns2Bprofiled, allminvalues)]
    # allmaxcounts = [data_df.where(col(x) == y).count() for x, y in zip(columns2Bprofiled, allmaxvalues)]
    df_counts = dprof_df[['column_names']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    # df_counts.insert(loc=0, column='counts_min', value=allmincounts)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)
    # df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)
    df_counts = df_counts[['column_names', 'min', 'max']]
    dprof_df = pd.merge(dprof_df, df_counts, on=['column_names'], how='left')
    # ==========================================
    # number of distinct values in each column
    dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
    # ============================================
    # most frequently occuring value in a column and its count
    dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count", ascending=False).limit(1). \
                                           toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'], axis=1)
    # least frequently occuring value in a column and its count
    dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count", ascending=True).limit(1). \
                                            toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'], axis=1)

    return dprof_df


if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
        .appName("ProfilerSpark") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_raw_df = load_survey_df(spark, sys.argv[1])
    df = survey_raw_df.repartition(2)
    df.show()

    start = time.time()
    cols2profile = df.columns  # select all or some columns from the table
    df_profile = dataprofile(df, cols2profile)
    print(df_profile.dtypes)
    df = pandas_to_spark(df_profile)
    df.show(truncate=False)
    end = time.time()
    print('Time taken to execute data profile function ', (end - start) / 60, ' minutes')
