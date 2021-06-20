import com.amazonaws.services.glue.DynamicFrame
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    
    val userSchema = new StructType()
      .add("Country", "string")
      .add("Customer", "string")
      .add("Description", "string")
      .add("InvoiceDate", "string")
      .add("InvoiceNo", "string")
      .add("Quantity", "string")
      .add("StockCode", "string")
      .add("UnitPrice", "string")
      .add("ingest_month", "integer")
      .add("ingest_day", "integer")
      .add("ingest_hour", "integer")
    // readstream() returns type DataStreamReader
    val datasource0 = sparkSession.readStream   
      .format("kinesis")
      .option("streamName", "MayaStream")
      .option("endpointUrl", "https://xxxxxx.us-east-2.amazonaws.com")
      .option("startingPosition", "TRIM_HORIZON")
      .load

    //cleanup
    val jsonEdits = datasource0.select(from_json($"data".cast("string"), userSchema).as ("data")).select("data.*")
      .withColumn("UnitPrice", $"UnitPrice".cast("double"))
      .withColumn("Quantity", $"Quantity".cast("integer"))
      .filter($"Quantity" > 0)
      .filter($"UnitPrice" > 0)
      .withColumn("InvoiceDate", unix_timestamp($"InvoiceDate", "MM/dd/yyyy HH:mm"))
      .withColumn("InvoiceDate", $"InvoiceDate".cast("timestamp"))
      .select($"Country", $"UnitPrice", $"Quantity", $"InvoiceDate")
      .filter($"Country" rlike "[A-Za-z]")
      
    val countDF = jsonEdits.withWatermark("InvoiceDate", "10 hours")
      .groupBy($"Country", window($"InvoiceDate", "1 hour"))
      .agg(sum(col("UnitPrice") * col("Quantity")) as "Total_sales")
      .select("Country", "window.start", "window.end", "Total_sales")

    val datasink2 = countDF.writeStream.foreachBatch { (dataFrame: Dataset[Row], batchId: Long) => {   //foreachBatch() returns type DataStreamWriter

      if (dataFrame.count() > 0) {
        dataFrame.write   
          .format("com.databricks.spark.redshift")
          .option("dbtable", "totalsales")     
          .option("url", "jdbc:redshift://xxxxxxxx.us-east-2.redshift.amazonaws.com:5439/dev?user=xxxxx&password=xxxxx") 
          .option("tempdir", "s3a://xxxxxx/totalsales") 
          .option("aws_iam_role", "arn:aws:iam::xxxxxx:role/red") 
          .mode(SaveMode.Append)
          .save()
      }
    }
    }
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", "s3a://xxxxxxx/totalsales_cp")
      .start()
    datasink2.awaitTermination()   
    
    Job.commit()
  }
}

