import time
import argparse

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# import numpy as np
# from scipy import stats


# @F.udf(FloatType())
# def median_udf(values):
#     return float(np.median(values))
@F.udf(FloatType())
def median_udf(values):
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 0:
        return None
    mid = n // 2
    if n % 2 == 0:
        return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0)
    else:
        return float(sorted_vals[mid])


# @F.udf(FloatType())
# def mode_udf(values):
#     return float(stats.mode(values).mode[0])


class ThresholdColumn:
    def __init__(self, column_name: str, threshold: float, is_maximum: bool):
        self.column_name = column_name
        self.threshold = threshold
        self.is_maximum = is_maximum


def parse_threshold_config(config_str):
    try:
        column_name, threshold, is_maximum = config_str.split(",")
        return ThresholdColumn(
            column_name, float(threshold), is_maximum.lower() == "true"
        )
    except Exception as e:
        raise argparse.ArgumentTypeError(
            f"Invalid threshold-config format: {config_str} ({e})"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_name", required=False, default="spark_app")
    parser.add_argument("--dataset_file", required=True)
    parser.add_argument("--threshold", action="append", type=parse_threshold_config)
    parser.add_argument("--group", action="append", type=str)
    args = parser.parse_args()
    app_name = args.app_name
    dataset_file = args.dataset_file
    dataset_path = f"hdfs://namenode:9000/input/{dataset_file}"
    # threshold_dict = dict(args.threshold)
    threshold_list = list(args.threshold)
    group_list = list(args.group)

    spark: SparkSession = SparkSession.builder.appName(app_name).getOrCreate()
    print("DEFAULT_PARALLELISM")
    print(spark.sparkContext.defaultParallelism)
    print("DEFAULT_PARALLELISM")
    dataset: DataFrame = (
        spark.read.option("inferSchema", True).option("header", True).csv(dataset_path)
    )
    dataset.printSchema()
    # print(dataset.head(5))
    start = time.time()

    threshold_df = dataset
    for thresh_col in threshold_list:
        if thresh_col.is_maximum:
            threshold_df = threshold_df.filter(
                f"{thresh_col.column_name} <= {thresh_col.threshold}"
            )
        else:
            threshold_df = threshold_df.filter(
                f"{thresh_col.column_name}>={thresh_col.threshold}"
            )

    threshold_df.write.format("json").mode("overwrite").save(
        f"hdfs://namenode:9000/output/threshold"
    )
    grouped_df = dataset.groupBy(group_list).agg(
        F.mean("GPA").alias(f"avg_GPA"),
        # F.median("GPA").alias(f"median_GPA"),
        # F.mode("GPA").alias(f"mode_GPA"),
        F.stddev_pop("GPA").alias(f"std_GPA"),
        F.var_pop("GPA").alias(f"var_GPA"),
        F.max("GPA").alias(f"max_GPA"),
        F.min("GPA").alias(f"min_GPA"),
    )
    gpa_list_df = dataset.groupBy(group_list).agg(
        F.collect_list("GPA").alias("gpa_list")
    )
    spread_df = gpa_list_df.select(
        *group_list,
        median_udf(F.col("gpa_list")).alias(f"median_GPA"),
        # mode_udf(F.col("gpa_list")).alias(f"mode_GPA"),
    )
    # mode_df_temp = dataset.groupBy(group_list + ["GPA"]).agg(
    #     F.count("*").alias("count")
    # )
    # mode_df_count = mode_df_temp.groupBy(group_list).agg(
    #     F.max("count").alias("max_count")
    # )
    # mode_df = (
    #     mode_df_temp.join(mode_df_count, on=group_list)
    #     .filter(F.col("count") == F.col("max_count"))
    #     .select(
    #         *group_list,
    #         F.col("GPA").alias("mode_GPA"),
    #         F.col("count").alias("mode_count"),
    #     )
    # )
    gpa_counts = dataset.groupBy(group_list + ["GPA"]).agg(
        F.count("*").alias("gpa_count")
    )
    window = Window.partitionBy(*group_list).orderBy(F.desc("gpa_count"))
    gpa_with_rank = gpa_counts.withColumn("rank", F.row_number().over(window))
    mode_df = gpa_with_rank.filter(F.col("rank") == 1).select(
        *group_list, F.col("GPA").alias("mode_GPA")
    )
    grouped_df = grouped_df.join(spread_df, on=group_list, how="left")
    grouped_df = grouped_df.join(mode_df, on=group_list, how="left")

    grouped_df.write.format("json").mode("overwrite").save(
        f"hdfs://namenode:9000/output/group"
    )

    perfect_by_goout = (
        dataset.select(col("GoOut"), col("GPA"))
        .filter("GPA=4.0")
        .groupBy("GoOut")
        .count()
        .withColumnRenamed("count", "perfect_gpa_count")
        .orderBy("GoOut", ascending=False)
    )
    total_by_goout = (
        dataset.groupBy("GoOut").count().withColumnRenamed("count", "total_count")
    )

    percentage_df = (
        total_by_goout.join(perfect_by_goout, on="GoOut", how="left")
        .fillna(
            0, subset=["perfect_gpa_count"]
        )  # In case some groups have no perfect GPAs
        .withColumn(
            "perfect_gpa_percentage",
            (col("perfect_gpa_count") / col("total_count")) * 100,
        )
        .orderBy("GoOut")
    )
    percentage_df.write.format("json").mode("overwrite").save(
        "hdfs://namenode:9000/output/GPA_by_GoOut"
    )

    end = time.time()
    print(end - start)
    spark.stop()
