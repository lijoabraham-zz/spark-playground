from pyspark.sql import Row
import json
import datetime
import pandas as pd
from pyspark.sql.functions import udf, array, dense_rank, expr, when
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from dependencies import spark as spk

class ETLJob(object):
    def main(self):
        """Main ETL script definition.
        :return: None
        """
        # start Spark application and get Spark session, logger and configs
        params = {'app_name': 'my_etl_job', 'files': ['configs/scrapper.json']}
        # spark, log, config = spk.SparkConnection(params)
        spark_details = spk.SparkConnection(params)

        spark = spark_details.spark
        log = spark_details.log

        # log that main ETL job is starting
        log.warn('etl_job is up-and-running')

        # execute ETL pipeline
        data = self.extract_data(spark)
        data_transformed = self.transform_data(data)
        self.load_data(data_transformed)
        # log the success and terminate Spark application
        log.warn('test_etl_job is finished')
        spark.stop()
        return None


    def extract_data(self, spark):
        """Load data from Parquet file format.
        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        dateValue = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        filename = "./data/mutual_funds_{}.json".format(dateValue)

        with open(filename) as f:
            lines = f.read().splitlines()
            df_inter = pd.DataFrame(lines)
            df_inter.columns = ['json_element']

        df_inter['json_element'].apply(json.loads)
        df_final = pd.json_normalize(df_inter['json_element'].apply(json.loads))

        df = spark.createDataFrame(df_final)
        for column in df.columns:
            if column.find('.') != -1:
                df = df.withColumnRenamed(column, column.replace('.', '_'))
        df = df.replace(float("nan"), 0)

        return df

    def calculate_growth(self, df):

            growth_1y = 1 if df[0] > df[4] else 0
            growth_3y = 1 if df[1] > df[5] else 0
            growth_5y = 1 if df[2] > df[6] else 0
            growth_7y = 1 if df[3] > df[7] else 0
            growth_10y = 1 if df[4] > df[8] else 0

            g_vals = [4, 3, 2, 1]
            # print(df)
            trend = 0
            for i in g_vals:
                diff = df[i - 1] - df[i]
                if diff > 0:
                    trend = trend + 1
                elif diff < 0:
                    trend = trend - 1
                # print(diff, df[i], df[i - 1], trend)

            growth = 5 - (growth_1y + growth_3y + growth_5y + growth_7y + growth_10y)
            trend_growth = 5 - trend

            return growth + trend_growth

    def transform_data(self, df):
        """Transform original dataset.
        :param df: Input DataFrame.
        :param steps_per_floor_: The number of steps per-floor at 43 Tanner
            Street.
        :return: Transformed DataFrame.
        """

        calculate_growth_udf = udf(lambda data: self.calculate_growth(data), IntegerType())

        df = df.withColumn("trend_growth", calculate_growth_udf(array([col for col in df.columns if col.find('growth') > 0])))
        df = df.withColumn("credit_rating_rank", dense_rank().over(Window.orderBy(df['credit_rating_AAA'].desc())))
        df = df.withColumn("asset_value_rank", dense_rank().over(Window.orderBy(df['asset_value'].desc())))
        df = df.withColumn("expense_ratio_rank", dense_rank().over(Window.orderBy(df['expense_ratio'].asc())))
        df = df.withColumn("risk_rank", when(df.risk == 'Low', 1).when(df.risk == 'Moderately Low', 3).otherwise(3))
        # risk parameters
        df = df.withColumn("sharpe_ratio_rank", dense_rank().over(Window.orderBy(df['fund_risk_sharpe'].desc())))
        df = df.withColumn("alpha_rank", dense_rank().over(Window.orderBy(df['fund_risk_alpha'].desc())))
        df = df.withColumn("beta_rank", dense_rank().over(Window.orderBy(df['fund_risk_beta'].asc())))
        df = df.withColumn("std_dev_rank", dense_rank().over(Window.orderBy(df['fund_risk_std_dev'].asc())))
        df = df.withColumn("mean_rank", dense_rank().over(Window.orderBy(df['fund_risk_mean'].desc())))

        df_transformed = df.withColumn('total_rank', expr('risk_rank + credit_rating_rank + asset_value_rank \
                                                        + expense_ratio_rank + sharpe_ratio_rank + alpha_rank+ \
                                                        beta_rank + std_dev_rank+ mean_rank + trend_growth'))
        return df_transformed.select('name', 'rating', 'total_rank').orderBy('total_rank')


    def load_data(self, df):
        """Collect data locally and write to CSV.
        :param df: DataFrame to print.
        :return: None
        """
        dateValue = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        (df
         .coalesce(1)
         .write
         .csv('output/mf-{}'.format(dateValue), mode='overwrite', header=True))
        return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    etl = ETLJob()
    etl.main()
