import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import scala.collection.JavaConverters._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window

import scala.collection.JavaConverters._


object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "bucket_name").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val BasePath = s"s3://${args("bucket_name")}/processed"
    val Basetable = DeltaTable.forPath(sparkSession, BasePath)
    val CheckpointDir = s"s3://${args("bucket_name")}/checkpoint2"

    val raworder = sparkSession.readStream.format("delta").load(s"s3://${args("bucket_name")}/raw/order/").withWatermark("timestamp", "2 hours")
    val rawmember= sparkSession.readStream.format("delta").load(s"s3://${args("bucket_name")}/raw/member/").withWatermark("timestamp", "3 hours") 
    

    


    def upsertIntoDeltaTable(updatedDf: DataFrame, batchId: Long): Unit = {
        // val groupDf = updatedDf.withColumn("timestamp",col("timestamp").cast(IntegerType)).groupBy("order_id").max("timestamp")
        // val processedDf = updatedDf.join(groupDf,groupDf("timestamp") ===  updatedDf("timestamp"),"leftsemi")
        // Use window function to get the latest order_id by timestamp
        val w = Window.partitionBy($"order_id").orderBy($"timestamp".desc)
        val Resultdf = updatedDf.withColumn("rownum", row_number.over(w)).where($"rownum" === 1).drop("rownum")
        
        // Merge from base with source
        Basetable.alias("b").merge(
            Resultdf.alias("s"), 
            "s.order_id = b.order_id")
            .whenMatched.updateAll()
            .whenNotMatched.insertAll()
            .execute()
    }
    


   val joinedorder = raworder.alias("order").join(
         rawmember.alias("membership"),
         expr("""
           order.order_owner = member.order_owner AND
           order.timestamp >= member.timestamp AND
           order.timestamp <= member.timestammp + interval 1 hour
         """)
   ).select($"order.order_id", $"order.order_owner", $"order.order_value", $"order.timestamp", $"member.membership", $"order.year", $"order.month", $"order.day", $"order.hour")



    val query = joinedorder
      .writeStream
      .format("delta")
      .foreachBatch(upsertIntoDeltaTable _)
      .option("checkpointLocation", CheckpointDir)
      .trigger(Trigger.Once())
      .outputMode("update")
      .start(s"s3://${args("bucket_name")}/processed")

    query.awaitTermination()   
    
    // deltaTable = DeltaTable.forPath(spark, "s3://delta-lake-streaming-demo/processed/")
    // deltaTable.generate("symlink_format_manifest")
    
    Job.commit()

  } 

}