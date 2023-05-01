#Not a full submission, for copy/paste into pyspark cli
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import pandas_udf
from typing import Iterator
from pyspark import TaskContext


spark.conf.set("spark.sql.shuffle.partitions",4)

hashedRows = spark.range(0,10000000).select(F.sha2(F.col('id').cast('binary'),256).alias('hash'))
hashedRows.unpersist()
hashedRows.cache()
hashedRows.count()

#Slower/shuffles everything to driver
keys = hashedRows.select('*',F.row_number().over(Window.orderBy(F.lit(None))).alias('key'))


#More distributed, still shuffles
hashPartition = hashedRows.select('*',F.spark_partition_id().alias('partition'))
partitionSizes = hashPartition.groupBy('partition').count()
almostAll = Window.orderBy(F.col('count')).rowsBetween(Window.unboundedPreceding, Window.currentRow-1)
startingIndex = F.broadcast(partitionSizes.select('partition',(F.coalesce(F.sum('count').over(almostAll),F.lit(0)).alias('startIndex'))))
computeKeyByPartitionWindow = Window.partitionBy('hp.partition').orderBy(F.lit(None))
keys2 = hashPartition.alias('hp').join(startingIndex.alias('si'),'partition').select('hash', (F.row_number().over(computeKeyByPartitionWindow) + F.col('startIndex') ).alias('key'))


#Pandas UDF can get with minimal shuffling
def getKeyF(offsetKeyByPartitionDict,initial=1):
   @pandas_udf('long')
   def _internal_udf(s: Iterator[pd.Series]) -> Iterator[pd.Series]:
      from pyspark import TaskContext
      curKey=initial
      pid = TaskContext.get().partitionId()
      for x in s:
         if  (x != pid).any(): raise RuntimeError('Partition Ids do not match!')
         offsetKey = offsetKeyByPartitionDict[pid]
         yield x.index+curKey+offsetKey
         curKey = curKey + x.size
   return _internal_udf


hashPartition = hashedRows.select('*',F.spark_partition_id().alias('partition'))
partitionSizes = hashPartition.groupBy('partition').count()
almostAll = Window.orderBy(F.col('partition')).rowsBetween(Window.unboundedPreceding, Window.currentRow-1)
#Get rolling sum to get which key each partition should start at. Technically, this does shuffle but it's relatively very small
startingKeyByPartition = dict(partitionSizes.select('partition',(F.coalesce(F.sum('count').over(almostAll),F.lit(0)).alias('startIndex'))).collect())
keys3 = hashPartition.select('hash', (getKeyF(startingKeyByPartition,1)('partition')).alias('key'))



#Just curious about using broadcast variables instead
@pandas_udf('long')
def getKeysForPartition(s: Iterator[pd.Series]) -> Iterator[pd.Series]:
   curIndex=initial.value
   for x in s:
      partitionSize = startingIndexByPartition.value[x[0]]
      yield x.index+curIndex+partitionSize
      curIndex = curIndex + x.size



hashPartition = hashedRows.select('*',F.spark_partition_id().alias('partition'))
partitionSizes = hashPartition.groupBy('partition').count()
almostAll = Window.orderBy(F.col('count')).rowsBetween(Window.unboundedPreceding, Window.currentRow-1)
startingIndexByPartition = sc.broadcast(dict(partitionSizes.select('partition',(F.coalesce(F.sum('count').over(almostAll),F.lit(0)).alias('startIndex'))).collect()))
initial = sc.broadcast(1)

computeKeyByPartitionWindow = Window.partitionBy('partition').orderBy(F.lit(None))
keys4 = hashPartition.select('hash', getKeysForPartition('partition').alias('key'))



#Approximate (can skip some with unequal partitions)
def getKeyF(numPartitions,initial=1):
   @pandas_udf('long')
   def _internal_udf(partitionNums: Iterator[pd.Series]) -> Iterator[pd.Series]:
      curKey=0
      for pn in partitionNums:
         yield (curKey+pn.index)*numPartitions+pn+initial
         curKey = curKey + pn.size
   return _internal_udf


keys5 = hashedRows.select('*', (getKeyF(hashedRows.rdd.getNumPartitions(),1)(F.spark_partition_id())).alias('key'))


keys5.write.mode('overwrite').csv('keys5')
keys5.unpersist()
keys5.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()
keys5.select('key',(F.col('key')-F.lag('key',1).over(Window.orderBy('key'))).alias('keyDif')).orderBy(F.abs(F.col('keyDif')-1).desc(),F.col('key').asc()).show()

byPartitionArbitraryOrder = Window.partitionBy(F.spark_partition_id()).orderBy(F.lit(0))
keys6 = hashedRows.withColumn('key', (F.row_number().over(byPartitionArbitraryOrder)*hashedRows.rdd.getNumPartitions()+F.spark_partition_id()).alias('key'))
keys6.write.mode('overwrite').csv('keys6')
keys6.unpersist()
keys6.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()
keys6.select('key',(F.col('key')-F.lag('key',1).over(Window.orderBy('key'))).alias('keyDif')).orderBy(F.abs(F.col('keyDif')-1).desc(),F.col('key').asc()).show()


#For validation
#keys.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()
keys.select('key',(F.col('key')-F.lag('key',1).over(Window.orderBy('key'))).alias('keyDif')).orderBy(F.abs(F.col('keyDif')-1).desc(),F.col('key').asc()).show()
#keys2.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()
#keys3.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()
keys3.select('key',(F.col('key')-F.lag('key',1).over(Window.orderBy('key'))).alias('keyDif')).orderBy(F.abs(F.col('keyDif')-1).desc(),F.col('key').asc()).show()
#keys4.groupBy('key').count().orderBy(F.col('count').desc(),F.col('key').asc()).show()

#Actual execution
#keys.cache()
#keys.count()
keys.write.mode('overwrite').csv('keys')
keys.unpersist()

#keys2.cache()
#keys2.count()
keys2.write.mode('overwrite').csv('keys2')
keys2.unpersist()

#keys3.cache()
#keys3.count()
keys3.write.mode('overwrite').csv('keys3')
keys3.unpersist()

#keys4.cache()
#keys4.count()
keys4.write.mode('overwrite').csv('keys4')
keys4.unpersist()
startingIndexByPartition.unpersist(True)
initial.unpersist(True)

#This is actually really bad? RDD->DF conversion slow
keys5 = hashedRows.rdd.zipWithIndex().toDF().select('_1.*',F.col('_2').alias('key'))
keys5.write.mode('overwrite').csv('keys5')
keys5.unpersist()



