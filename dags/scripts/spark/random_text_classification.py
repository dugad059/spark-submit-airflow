# pyspark
import argparse

from pyspark.sql import SparkSession


def random_text_classifier(input_loc, output_loc):


    # read input
    df_spark = spark.read.option("header", True).csv(input_loc)

    df_spark_drop=df_spark.drop('Set Order', 'Distance', 'Seconds', 'Workout Notes', 'RPE')

    df_spark_updated=df_spark_drop.na.fill('-')

    # perform table cleaning

    # parquet is a popular column storage format, we use it here
    df_spark_updated.write.mode("overwrite").parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/strong")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    random_text_classifier(input_loc=args.input, output_loc=args.output)
