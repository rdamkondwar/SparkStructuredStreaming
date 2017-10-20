import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType

object PartAQuestion3 {
   def main(args: Array[String]) {
   val dirPath: String = args(0)
   val spark = SparkSession.builder.appName("PartAQuestion3")
   //.config("spark.eventLog.enabled", "true")
   //.config("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
   .config("spark.sql.streaming.checkpointLocation", ".")
   .getOrCreate()

   //spark.sparkContext.setCheckpointDir(".")

   spark.sparkContext.setLogLevel("ERROR")

   val userSchema = new StructType().add("userA", "long").add("userB", "long").add("timestamp", "timestamp").add("action", "string")

   val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv(dirPath)


   val wordCounts = csvDF.groupBy("userA").count()

   // val length: Long = (wordCounts.groupBy().count().writeStream.start()).first().get(0).asInstanceOf[Long]


   val query = wordCounts.writeStream.trigger(ProcessingTime(5000)).format(classOf[CustomConsoleSink].getCanonicalName).outputMode("complete").start()
   // val query = wordCounts.writeStream.trigger(ProcessingTime(5000)).format("console").option("numRows", length).outputMode("complete").start()
   query.awaitTermination()
   }
}

