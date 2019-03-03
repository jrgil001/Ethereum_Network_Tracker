import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def format_function(receiptJSON):
    formattedJSON = {
        "status" : 0,
        "gasUsed" : 0,
        "cumulativeGasUsed" : 0
    }
    formattedJSON['status'] = int(receiptJSON.__getitem__("status"), 16)
    formattedJSON['gasUsed'] = int(receiptJSON.__getitem__("gasUsed"), 16)
    formattedJSON['cumulativeGasUsed'] = int(receiptJSON.__getitem__("cumulativeGasUsed"), 16)

    return formattedJSON

if __name__ == "__main__":
    start = time.time()
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    spark = SparkSession.builder.appName("ETL for transactions data preprocessing").config("spark.some.config.option", "some-value").getOrCreate()
    receipts_df = spark.read.json("/home/jrgil/Downloads/2019-03-02_rec/*.json")
    status_gas_df = receipts_df.select(receipts_df["status"], receipts_df["gasUsed"], receipts_df["cumulativeGasUsed"])
    number_rows = status_gas_df.count()
    receipts_rdd = status_gas_df.rdd
    receipts_rdd.map(lambda receipt: format_function(status_gas_df))
    results = receipts_rdd.collect()
    
    f = open('/home/jrgil/Downloads/2019-03-02_rec/results.txt',"w+")
    i = 0
    for receipt in results:
        try:
            receipt_status = int(receipt.__getitem__("status"), 16)
            receipt_gasUsed = int(receipt.__getitem__("gasUsed"), 16)
            receipt_cumulativeGasUsed = int(receipt.__getitem__("cumulativeGasUsed"), 16)
            # receipt_status = receipt.__getitem__("status")
            # receipt_gasUsed = receipt.__getitem__("gasUsed")
            # receipt_cumulativeGasUsed = receipt.__getitem__("cumulativeGasUsed")
            # receipt_status = float.fromhex(receipt.__getitem__("status"))
            # receipt_gasUsed = float.fromhex(receipt.__getitem__("gasUsed"))
            # receipt_cumulativeGasUsed = float.fromhex(receipt.__getitem__("cumulativeGasUsed"))

            f.write(str(receipt_status) + ' ' + str(receipt_gasUsed)+ ' ' + str(receipt_cumulativeGasUsed))
            if(i < number_rows-1):
                f.write(" \n")
            i = i + 1
        except:
            print("An exception occurred:"+str(i))

    f.close()

    spark_context.stop()

    end = time.time()
    print("****************************************************TIME: " + str(end - start))