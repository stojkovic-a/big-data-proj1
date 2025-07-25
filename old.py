average_age = dataset.select(col("Age")).alias("average_age").agg({"Age": "avg"})
    avg_age_val = average_age.collect()[0][0]
    average_age.write.format("json").mode("overwrite").save(
        "hdfs://namenode:9000/output/average_age"
    )

    average_grade_young = (
        dataset.select(col("Age"), col("GPA"))
        .filter(f"Age>{avg_age_val}")
        .agg({"GPA": "avg"})
    )
    avg_grade_young_val = average_grade_young.collect()[0][0]
    average_grade_old = (
        dataset.select(col("Age"), col("GPA"))
        .filter(f"Age<{avg_age_val}")
        .agg({"GPA": "avg"})
    )
    avg_grade_old_val = average_grade_old.collect()[0][0]

    summary_df = spark.createDataFrame(
        [
            Row(
                avg_age=avg_age_val,
                avg_gpa_young=avg_grade_young_val,
                avg_gpa_old=avg_grade_old_val,
            )
        ]
    )
    summary_df.write.format("json").mode("overwrite").save(
        "hdfs://namenode:9000/output/average_gpa"
    )

    average_grade_goout = (
        dataset.select(col("GoOut"), col("GPA")).groupBy("GoOut").agg({"GPA": "avg"})
    )
    average_grade_goout.write.format("json").mode("overwrite").save(
        "hdfs://namenode:9000/output/gpa_goout"
    )

    perfect_by_goout = (
        dataset.select(col("GoOut"), col("GPA"))
        .filter("GPA=4.0")
        .groupBy("GoOut")
        .count()
        .withColumnRenamed("count", "perfect_gpa_count")
        .orderBy("GoOut", ascending=False)
    )
    print(perfect_by_goout.collect())

    total_by_goout = (
        dataset.groupBy("GoOut").count().withColumnRenamed("count", "total_count")
    )
    print(total_by_goout.collect())

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

############################################



    average_age = dataset.select(col("Age")).alias("average_age").agg({"Age": "avg"})
    avg_age_val = average_age.collect()[0][0]
    average_age.write.format("json").mode("overwrite").save("./average_age")

    average_grade_young = (
        dataset.select(col("Age"), col("GPA"))
        .filter(f"Age>{avg_age_val}")
        .agg({"GPA": "avg"})
    )
    avg_grade_young_val = average_grade_young.collect()[0][0]
    average_grade_old = (
        dataset.select(col("Age"), col("GPA"))
        .filter(f"Age<{avg_age_val}")
        .agg({"GPA": "avg"})
    )
    avg_grade_old_val = average_grade_old.collect()[0][0]

    summary_df = spark.createDataFrame(
        [
            Row(
                avg_age=avg_age_val,
                avg_gpa_young=avg_grade_young_val,
                avg_gpa_old=avg_grade_old_val,
            )
        ]
    )
    summary_df.write.format("json").mode("overwrite").save("./average_gpa")

    average_grade_goout = (
        dataset.select(col("GoOut"), col("GPA")).groupBy("GoOut").agg({"GPA": "avg"})
    )
    average_grade_goout.write.format("json").mode("overwrite").save("./gpa_goout")

    perfect_by_goout = (
        dataset.select(col("GoOut"), col("GPA"))
        .filter("GPA=4.0")
        .groupBy("GoOut")
        .count()
        .withColumnRenamed("count", "perfect_gpa_count")
        .orderBy("GoOut", ascending=False)
    )
    print(perfect_by_goout.collect())

    total_by_goout = (
        dataset.groupBy("GoOut").count().withColumnRenamed("count", "total_count")
    )
    print(total_by_goout.collect())

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
    percentage_df.write.format("json").mode("overwrite").save("./GPA_by_GoOut")
    end = time.time()
    print(end - start)
    spark.stop()
