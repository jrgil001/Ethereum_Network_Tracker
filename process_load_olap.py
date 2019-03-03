import pandas as pd
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
defConf = spark.sparkContext._conf.getAll()
conf = spark.sparkContext._conf.set('spark.cores.max', '16')
spark.sparkContext.stop()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

start = time.time()

blocks_df = spark.read.json("/home/jrgil/eht_proj/2019-03-02/*.json")

blocks_nums_collection = blocks_df.select("number").collect()
transactions_df = pd.DataFrame()
receipts_df = pd.DataFrame()
receipts_schema = StructType([])

for block_num in blocks_nums_collection:
    try:
        int_block_num = int(block_num.__getitem__("number"), 16)
        current_transactions_df = spark.read.json("/home/jrgil/eht_proj/2019-03-02/" + str(int_block_num) + "/*.json")
        trans_frames = [transactions_df, current_transactions_df.toPandas()]
        transactions_df = pd.concat(trans_frames)
        current_receipts_df = spark.read.json("/home/jrgil/eht_proj/2019-03-02/" + str(int_block_num) + "_rec/*.json")
        receipts_schema = current_receipts_df.schema
        recs_frames = [receipts_df, current_receipts_df.toPandas()]
        receipts_df = pd.concat(recs_frames)
    except:
        print("An exception occurred********************************************")

transactions_df.set_index(['blockHash', 'hash'])
receipts_df.set_index(['blockHash', 'transactionHash'])

blocks_df.createOrReplaceTempView("blocks")
transactions_sql = spark.createDataFrame(transactions_df)
transactions_sql.createOrReplaceTempView("transactions")
receipts_sql = spark.createDataFrame(receipts_df, receipts_schema)
receipts_sql.createOrReplaceTempView("receipts")
block_trans = spark.sql("SELECT t.blockHash, b.timestamp, t.hash as transactionHash, t.from, t.to, t.value, t.gasPrice FROM blocks b inner join transactions t on b.hash = t.blockHash")
block_trans.createOrReplaceTempView("block_trans")
blocks_transactions_receipts = spark.sql("SELECT * FROM block_trans bt inner join receipts r on bt.transactionHash = r.transactionHash")
facts = blocks_transactions_receipts.collect()
for fact in facts:
    print("from: " + str(int(fact[3], 16)) + " to: " + str(int(fact[4], 16)) + " value: " + str(int(fact[5], 16)))


end = time.time()
print("****************************************************TIME: " + str(end - start))