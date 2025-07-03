# Assume you have all the following 3 dataframes available in session
# txnDF, modeDF and statusDF
# Following questions need to be coded for.

# 1. dataframe to rdd
rdd=df.rdd
print(rdd.collect())

# 2. join transactions with modes and status
txnDF = txnDF.join(modeDF, txnDF.mode == modeDF.mode, how="inner")
txnDF = txnDF.join(statusDF, txnDF.status == statusDF.statusid, how="inner")
joinedDF = txnDF \
    .join(modeDF, txnDF.mode == modeDF.modeid, "left") \
    .join(statusDF, txnDF.status == statusDF.statusid, "left") \
    .select("txnid", "amount", "date", "statusname", "modename")
# 3. get count of success and failure transactions
txnDF.groupBy("status").count().show()

success_failure_counts = joinedDF \
    .filter(col("statusname").isin("success", "failure")) \
    .groupBy("statusname") \
    .count()
# 4. sort modename basis count of success transactions
success_by_mode = joinedDF \
    .filter(col("statusname") == "success") \
    .groupBy("modename") \
    .count() \
    .orderBy(col("count").desc())

# 5. candidate for partition
Date 

# 6. when transaction are at scale
