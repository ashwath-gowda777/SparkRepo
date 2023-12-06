from pyspark.sql.functions import *

#
def unique_location_count(df_users, df_transactions, spark):
    df_users = df_users.select([col(c).alias(c.replace(" ", "")) for c in df_users.columns])
    df_transactions = df_transactions.select([col(c).alias(c.replace(" ", "")) for c in df_transactions.columns])

    df_join = df_users.join(df_transactions, df_users.user_id == df_transactions.userid, "inner")
    result_df1 = df_join.select("product_id", "location").distinct().groupBy("product_id", "location").agg(count("location").alias("count_unique_locations"))
    return result_df1

def products_bought(df_users, df_transactions, spark):
    df_users = df_users.select([col(c).alias(c.replace(" ", "")) for c in df_users.columns])
    df_transactions = df_transactions.select([col(c).alias(c.replace(" ", "")) for c in df_transactions.columns])

    df_join = df_users.join(df_transactions, df_users.user_id == df_transactions.userid, "inner")
    result_df2 = df_join.groupBy("userid").agg(collect_list("product_id").alias("products"))
    return result_df2

def total_spending(df_users, df_transactions, spark):
    df_users = df_users.select([col(c).alias(c.replace(" ", "")) for c in df_users.columns])
    df_transactions = df_transactions.select([col(c).alias(c.replace(" ", "")) for c in df_transactions.columns])

    df_join = df_users.join(df_transactions, df_users.user_id == df_transactions.userid, "inner")
    result_df3 = df_join.groupBy("user_id", "product_id").agg(sum("price").alias("Total_Spendings"))
    return result_df3
