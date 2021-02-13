import org.apache.spark.sql.SparkSession

object showData extends App {
  override def main(args: Array[String]) {

    //val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()
/*
    val path = args(0)
    val dataFrames = args.drop(1) map (tempFile => sparkSession.read.parquet(path+tempFile))

 */

    //val inputFile = sparkContext.textFile("file.txt").collect

    /*val dataFrames = inputFile.map(line => {
        println(path + line)
        sparkSession.read.parquet(path + line)
      }
    )*/

    //merge dei parquet in un dataFrame unico

    /*
    val dataFrameTotal = dataFrames.reduce(_ union _)

    dataFrameTotal.coalesce(1).write.parquet("s3n://wtt-s3-1/provaOutput")

     */

    sparkContext.parallelize(Array[String]("pippo", "pluto", "paperino")).coalesce(1).saveAsTextFile("s3n://wtt-s3-1/exp/fileSingolo.txt")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}
