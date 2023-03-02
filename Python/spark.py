'''
  執行環境: Azure Databricks Notebook
'''

# 如何讀 delta lake table，並呈現
df = spark.sql('SQL語句') # return pyspark.sql.dataframe
display(df)

# 如何取出特定欄位的唯一值
distinct_values = df.select('欄位名稱').distinct().collect()
# 如何個別讀出(spark) Iterate Over Rows And Columns In The PySpark DataFrame
## row 是一個 pyspark.sql.types.Row
result_lst = [row.欄位名稱 for row in distinct_values]
## row_iterator 是一個 pyspark.sql.types.Row
for row_iterator in df.collect():
    print(row_iterator['column'])


# 如何取出特定欄位組合唯一值，併排序
df.select('欄位名稱','欄位名稱').distinct().sort("欄位名稱").show()

# filter
df.filter((df.欄位名稱=='值'))
