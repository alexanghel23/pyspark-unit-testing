import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def sample_transform(input_df: DataFrame) -> DataFrame:
    inter_df = input_df.where(input_df['race'] == \
                              F.lit('hobbit')).groupBy('name').agg(F.sum('coins').alias('total_coins'))
    output_df = inter_df.select('name', 'total_coins', \
                                F.when(F.col('total_coins') > 10, 'yes').otherwise('no').alias('indicator')).where(
                F.col('indicator') == F.lit('yes'))
    return output_df