import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from soda.scan import Scan


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("spark_tests") \
      .getOrCreate()


def build_scan(name, spark_session):
    scan = Scan()
    scan.disable_telemetry()
    scan.set_scan_definition_name("data_quality_test")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark_session)
    return scan


def test_videos_source(spark):
    videos_df = spark.read.option('header', 'true')\
        .option("inferSchema", "true")\
        .csv('data_quality/datasets/USvideos.csv')
    videos_df.createOrReplaceTempView('videos')

    scan = build_scan("videos_source_data_quality_test", spark)
    scan.add_sodacl_yaml_file("data_quality/videos_checks.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()


def test_comments_source(spark):
    comments = spark.read.option('header', 'true')\
        .option("inferSchema", "true")\
        .csv('data_quality/datasets/UScomments.csv')
    comments.createOrReplaceTempView('comments')

    scan = build_scan("comments_source_data_quality_test", spark)
    scan.add_sodacl_yaml_file("data_quality/comments_checks.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()


def test_comments_clean_source(spark):
    comments_schema = StructType([ \
        StructField("video_id", StringType(), True), \
        StructField("comment_text", StringType(), True), \
        StructField("likes", IntegerType(), True), \
        StructField("replies", IntegerType(), True)])
    comments_clean = spark.read.option('header', 'true')\
        .option("mode", "DROPMALFORMED")\
        .schema(comments_schema).csv('data_quality/datasets/UScomments.csv')
    comments_clean.createOrReplaceTempView('comments_clean')

    scan = build_scan("comments_clean_source_data_quality_test", spark)
    scan.add_sodacl_yaml_file("data_quality/comments_clean_checks.yml")

    scan.execute()

    scan.assert_no_checks_warn_or_fail()