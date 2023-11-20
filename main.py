import os
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, desc, dense_rank
from pyspark.sql.window import Window
from zipfile import ZipFile
from datetime import timedelta


def average_trip_duration_per_day(trips_df):
    result_df = (
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .agg({"tripduration": "avg"})
        .orderBy("day")
    )
    result_df.write.csv("reports/average_trip_duration_per_day", header=True, mode="overwrite")


def trips_count_per_day(trips_df):
    result_df = (
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .count()
        .orderBy("day")
    )
    result_df.write.csv("reports/trips_count_per_day", header=True, mode="overwrite")


def most_popular_starting_station_per_month(trips_df):
    result_df = (
        trips_df.withColumn("month", date_format("start_time", "yyyy-MM"))
        .groupBy("month", "from_station_name")
        .count()
        .orderBy("month", col("count").desc())
        .groupBy("month")
        .agg({"from_station_name": "first"})
        .orderBy("month")
        .withColumnRenamed("first(from_station_name)", "most_popular_starting_station")
    )
    result_df.write.csv("reports/most_popular_starting_station_per_month", header=True, mode="overwrite")


def top_three_stations_per_day_last_two_weeks(trips_df):
    df = trips_df.withColumn("start_time", col("start_time").cast("timestamp"))
    end_date = df.agg({"start_time": "max"}).collect()[0][0]
    start_date = end_date - timedelta(days=14)
    df = df.filter((col("start_time") >= start_date) & (col("start_time") <= end_date))
    station_counts = df.groupBy("from_station_id", "from_station_name").count()
    window_spec = Window.orderBy(desc("count"))
    ranked_stations = station_counts.withColumn("rank", dense_rank().over(window_spec))
    top_stations = ranked_stations.filter(col("rank") <= 3)
    top_stations.write.csv("reports/top_three_stations_per_day_last_two_weeks", header=True, mode="overwrite")


def average_trip_duration_by_gender(trips_df):
    result_df = (
        trips_df.groupBy("gender")
        .agg({"tripduration": "avg"})
        .orderBy("gender")
    )
    result_df.write.csv("reports/average_trip_duration_by_gender", header=True, mode="overwrite")


def age_stats_top_ten(trips_df):
    result_df = (
        trips_df.groupBy("birthyear")
        .agg({"tripduration": "avg"})
        .orderBy(col("avg(tripduration)").desc())
        .limit(10)
    )
    result_df.write.csv("reports/age_stats_top_ten", header=True, mode="overwrite")


def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]

    csv_data = {}
    for file in files:
        if file.lower().endswith('.csv'):
            csv_content = file_obj.read(file)
            csv_data[file] = pd.read_csv(io.StringIO(csv_content.decode('ISO-8859-1')))

    return csv_data


def process_zip_files(data_path):
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    sc = spark.sparkContext

    zips = sc.binaryFiles(f"{data_path}/*.zip")

    files_data = zips.map(zip_extract).collect()

    for data in files_data:
        for file, content in data.items():
            if content.empty:
                print(f"Warning: CSV file {file} is empty.")
            else:
                if 'merged_df' not in locals():
                    merged_df = spark.createDataFrame(content)
                # else:
                #     merged_df = merged_df.union(spark.createDataFrame(content))

    merged_df.show()

    try:
        average_trip_duration_per_day(merged_df)
        trips_count_per_day(merged_df)
        most_popular_starting_station_per_month(merged_df)
        top_three_stations_per_day_last_two_weeks(merged_df)
        average_trip_duration_by_gender(merged_df)
        age_stats_top_ten(merged_df)

    finally:
        spark.stop()


def main():
    data_folder = 'data'
    current_directory = os.getcwd()
    data_path = os.path.join(current_directory, data_folder)
    process_zip_files(data_path)


if __name__ == "__main__":
    main()
