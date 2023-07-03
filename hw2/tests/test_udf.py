from video_analytics.functions import pandas_split, numpy_median, sum_func
from chispa import *
from pyspark.sql.types import *
import pyspark.sql.functions as f


def test_tags(spark):
    input_data = [
        ("Late night|Seth Meyers|check in|hud|Ben Carson|NBC|NBC TV|television",),
        ("170912 BTS",),
        ("apple iphone x hands on|Apple iPhone X|iPhone X|apple iphone x first look",),
        ("Maru|cat|kitty|pets|まる|猫|ねこ",),
        ("BIGHIT|빅히트|방탄소년단|BTS|BANGTAN|방탄",),
        ("Gran Turismo|グランツーリスモ|Gran Turismo Sport|グランツーリスモＳＰＯＲＴ|GTS",),
        ("|||",),
        ("|",),
        ("",),
        (None,)
    ]

    expected_data = [
        (["Late night", "Seth Meyers", "check in", "hud", "Ben Carson", "NBC", "NBC TV", "television"],),
        (["170912 BTS"],),
        (["apple iphone x hands on", "Apple iPhone X", "iPhone X", "apple iphone x first look"],),
        (["Maru", "cat", "kitty", "pets", "まる", "猫", "ねこ"],),
        (["BIGHIT", "빅히트", "방탄소년단", "BTS", "BANGTAN", "방탄"],),
        (["Gran Turismo", "グランツーリスモ", "Gran Turismo Sport", "グランツーリスモＳＰＯＲＴ", "GTS"],),
        (["", "", "", ""],),
        (["", ""],),
        ([""],),
        (None,)
    ]

    schema=['tags']
    
    df = spark \
        .createDataFrame(data=input_data, schema=schema) \
        .withColumn("tags", pandas_split(f.col("tags")))
    
    expected_df = spark\
        .createDataFrame(data=expected_data, schema=schema)

    assert_df_equality(df, expected_df)


def test_median(spark):
    input_data = [
        ("Shows", 1.0,),
        ("Shows", 1.0,),
        ("Gaming", 100.0),
        ("Gaming", 50.0),
        ("Education", 50.0),
        ("Education", 0.0),
        ("Sports", None),
        ("Sports", 10.0)
    ]

    expected_data = [
        ("Shows", 1.0,),
        ("Gaming", 75.0),
        ("Education", 25.0),
        ("Sports", None)
    ]

    schema = StructType([ \
        StructField("category", StringType(),True), \
        StructField("score", DoubleType(),True)
    ])
    
    df = spark \
        .createDataFrame(data=input_data, schema=schema) \
        .groupBy("category").agg(numpy_median("score").alias("score"))
    
    expected_df = spark\
        .createDataFrame(data=expected_data, schema=schema)

    assert_df_equality(df, expected_df, ignore_row_order=True)


def test_sum_func(spark):
    
    data = [
        (1, 1, 1, 1, 1, 50),
        (0, 0, 0, 0, 0, None),
        (100, 10, 10, 100, 0, 90)
    ]

    schema=["likes", "dislikes", "views", "comment_likes", "comment_replies", "expected_score"]
    
    df = spark.createDataFrame(data, schema) \
        .withColumn("score", sum_func("likes", "dislikes", "views", "comment_likes", "comment_replies"))
    
    assert_column_equality(df, "score", "expected_score")
