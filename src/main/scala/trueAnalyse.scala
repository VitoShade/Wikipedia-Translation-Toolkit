import Utilities._
import org.apache.spark.sql.SparkSession

object trueAnalyse extends App {

  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[32]").appName("trueAnalyse").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName   = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/indici"
    val tempFolderName    = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/tempResult"
    val outputFolderName  = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/result"
    val errorFolderName   = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/result/error"
    val parquetFolder     = ""
    val folderSeparator   = "/"

    //dataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(parquetFolder), "en", sparkSession)


    val startTime = System.currentTimeMillis()








    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}