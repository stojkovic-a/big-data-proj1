import sys
from pyspark.sql import SparkSession, DataFrame


if __name__ == "__main__":
    # check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: dataframe_example.py <input folder> ")
        exit(-1)
    # Set a name for the application
    appName = "DataFrame Example"
    input_folder = sys.argv[1]
    print(input_folder)
    spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    dataset: DataFrame = (
        spark.read.option("inferSchema", True).option("header", True).csv(input_folder)
    )
    dataset.show(10, False)
    # dataset.rdd
    dataset.printSchema()
    dataset.write.format("csv").mode("append").save("./test")
    spark.stop()
