from pyspark.sql import SparkSession
from utils import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Question1").getOrCreate()

    df1_users = spark.read.csv(r"C:\Users\Admin\OneDrive\Desktop\Diggibyte_Intership\4_Spark\Spark (1)\Spark\spark_1\user.csv", header=True, inferSchema=True)
    df1_trn = spark.read.csv(r"C:\Users\Admin\OneDrive\Desktop\Diggibyte_Intership\4_Spark\Spark (1)\Spark\spark_1\transaction.csv", header=True, inferSchema=True)

    result_1 = unique_location_count(df1_users, df1_trn,spark)
    result_2 = products_bought(df1_users, df1_trn,spark)
    result_3 = total_spending(df1_users, df1_trn, spark)

    result_1.show()
    result_2.show()
    result_3.show()

    spark.stop()

