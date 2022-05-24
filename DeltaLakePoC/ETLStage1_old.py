import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Import the packages
from delta import *
from pyspark.sql.session import SparkSession
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'bootstrap_servers', 'topic'])


sc = SparkContext()
sc.setSystemProperty('spline.mode','REQUIRED')
sc.setSystemProperty('spline.producer.url','http://52.86.213.35:48080/producer')
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields",2000)
sc._jvm.za.co.absa.spline.harvester.SparkLineageInitializer.enableLineageTracking(spark._jsparkSession)
glueContext = GlueContext(sc)

#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


data_bucket = args['bucket_name']
bootstrap_servers = args['bootstrap_servers']
topic = args['topic']

schema = StructType([ \
  StructField("order_id", IntegerType(), True), \
  StructField("order_owner", StringType(), True), \
  StructField("order_value", IntegerType(), True), \
  StructField("timestamp", TimestampType(), True), ])

# Initialize Spark Session along with configs for Delta Lake
# spark = SparkSession \
#     .builder \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

def insertToDelta(microBatch, batchId):  
  date = datetime.today()
  year = date.strftime("%y")
  month = date.strftime("%m")
  day = date.strftime("%d")
  hour = date.strftime("%H")
  if microBatch.count() > 0:
      
    columns = ["order_id", "product_name"]
    data = [(4, "XBox"), (5, "PS5"), (0, "Saga"), (2, "Nintendo")]

    initial_rdd = spark.sparkContext.parallelize(data)
    initial_df = initial_rdd.toDF(columns).createOrReplaceTempView("ou")
    #initial.show(false) 
    df = microBatch.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day)).withColumn("hour", lit(hour))
    df.createOrReplaceTempView("inn")
    joined = spark.sql("""select inn.order_id, ou.product_name from inn inner join ou on inn.order_id = ou.order_id""")
  
    #joined = initial_df.join(microBatch, initial_df.order_id == microBatch.order_id).select(microBatch["*"], initial["product_name"])
    
    #df = joined.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day)).withColumn("hour", lit(hour))
    joined.write.partitionBy("year", "month", "day", "hour").mode("append").format("delta").save(f"s3://{data_bucket}/raw11/")


options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM", 
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }  

# Read Source
df = spark \
  .readStream \
  .format("kafka") \
  .options(**options) \
  .option("kafka.bootstrap.servers", bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .option("maxOffsetsPerTrigger", 1000) \
  .load().select(col("value").cast("STRING"))

df2 = df.select(from_json("value", schema).alias("data")).select("data.*")


# Write data as a DELTA TABLE
df3 = df2.writeStream \
  .foreachBatch(insertToDelta) \
  .option("checkpointLocation", f"s3://{data_bucket}/checkpoint11/") \
  .trigger(processingTime="60 seconds") \
  .start()

df3.awaitTermination()

job.commit()