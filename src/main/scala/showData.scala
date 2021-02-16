import org.apache.spark.sql.SparkSession

object showData extends App {
  override def main(args: Array[String]) {

    //val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()
    //raccolta di tutti i file .txt nella cartella di input
    val nFile = args.drop(1).size
    val bucket = args(0)


    val errorFolderName   = bucket + "error"
    val folderSeparator   = "/"

    // Unione dei DataFrame dai parquet inglesi

    val dataFramesEn = args.slice(1, nFile/2+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))


    val dataFrameSrc = dataFramesEn.reduce(_ union _)

    // Unione dei DataFrame italiani
    val dataFramesIt = args.slice(nFile/2+1, nFile+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))

    val dataFrameDst = dataFramesIt.reduce(_ union _)


    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}
