import pandas as pd
from numpy import median as nmedian
from pyspark.sql.functions import *
from pyspark.sql.types import *


@pandas_udf(ArrayType(StringType()), functionType=PandasUDFType.SCALAR)
def pandas_split(tags_string: pd.Series) -> pd.Series:
    return tags_string.str.split('|')

@pandas_udf(DoubleType(), functionType=PandasUDFType.GROUPED_AGG)
def numpy_median(score: pd.Series) -> pd.Series:
    return nmedian(score)

@pandas_udf(LongType())
def sum_func(likes: pd.Series, 
             dislikes: pd.Series,
             views: pd.Series,
             comment_likes: pd.Series,
             comment_replies: pd.Series,) -> pd.Series:
    return ((likes*100/(likes + dislikes + 1)) + 
            ((comment_replies + comment_likes)*100/(views + comment_likes + comment_replies)))/2