import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType

object PartAQuestion2 {
   def main(args: Array[String]) {
   val dirPath: String = args(0)
   val spark = SparkSession.builder.appName("PartAQuestion2")
   //.config("spark.eventLog.enabled", "true")
   //.config("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
   .getOrCreate()

   spark.sparkContext.setLogLevel("ERROR")

   val userSchema = new StructType().add("userA", "long").add("userB", "long").add("timestamp", "timestamp").add("action", "string")

   val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv(dirPath)

   val wordCounts = csvDF.select("userB").where("action = 'MT'")

   val query = wordCounts.writeStream.trigger(ProcessingTime(10000)).format(classOf[CustomConsoleSink].getCanonicalName).option("checkpointLocation", false).start()
   query.awaitTermination()
   }
}

