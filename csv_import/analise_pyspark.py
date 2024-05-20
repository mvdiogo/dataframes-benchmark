from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when
import time

def load_and_clean_data(spark, file_path, schema):
    try:
        start_load_time = time.time()
        print("Loading data...")
        df = spark.read.csv(file_path, header=True, schema=schema)
        print("Data loaded successfully.")
        end_load_time = time.time()
        load_duration = end_load_time - start_load_time

        start_clean_time = time.time()
        print("Cleaning data...")
        df = df.dropna()
        print("Data cleaned successfully.")
        end_clean_time = time.time()
        clean_duration = end_clean_time - start_clean_time

        return df, load_duration, clean_duration
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None, None

def main():
    spark = SparkSession.builder.appName("Data8317Analysis").getOrCreate()
    file_path = 'Data8317.csv'
    schema = "Year int, Age int, Ethnic int, Sex int, Area int, count int"
    current_time = time.strftime("%Y%m%d%H%M%S")

    df, load_duration, clean_duration = load_and_clean_data(spark, file_path, schema)

    if df is None:
        return

    operations = [
        ('Sum', lambda df: df.select(sum(col("count")).alias("total_count"))),
        ('Count', lambda df: df.select(count("*").alias("total_count"))),
        ('Groupby Age Sum', lambda df: df.groupBy("Age").agg(sum("count").alias("sum_count"))),
        ('Filter Age > 0', lambda df: df.filter(col("Age") > 0)),
        ('Select Year and Count', lambda df: df.select("Year", "count"))
    ]

    report_data = []
    report_data.append(["Load", load_duration])
    report_data.append(["Clean", clean_duration])
    for op_name, op in operations:
        start_time = time.time()
        result = op(df)
        duration = time.time() - start_time
        print(f"{op_name} completed in {duration} seconds.")
        result.show(5)  # Show first 5 rows of the result
        report_data.append([op_name, duration])

    # No need for garbage collection in PySpark, Spark manages memory

    # Create a PySpark DataFrame for the report
    report_df = spark.createDataFrame(report_data, ["Operation", "Time (seconds)"])
    report_df.coalesce(1).write.csv(f'results_pyspark_{current_time}_report', header=True, mode='overwrite')

    spark.stop()

if __name__ == '__main__':
    main()