import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object PartAQuestion1 {
   def main(args: Array[String]) {
   val dirPath: String = args(0)
   val spark = SparkSession.builder.appName("PartAQuestion1")
   .config("spark.eventLog.enabled", "true")
   .config("spark.eventLog.dir", "hdfs://10.254.0.146/spark/history")
   .getOrCreate()
   import spark.implicits._

   val userSchema = new StructType().add("userA", "long").add("userB", "long").add("timestamp", "timestamp").add("action", "string")

   val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv(dirPath)

   val wordCounts = csvDF.groupBy(window($"timestamp", "60 minutes", "30 minutes"),$"action").count()
   val query = wordCounts.writeStream.outputMode("complete").format("console").option("numRows", 100).option("truncate", false).start()
   query.awaitTermination()
   }
}

