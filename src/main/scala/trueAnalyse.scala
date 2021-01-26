import Utilities._
import org.apache.spark.sql.SparkSession

object analyseData extends App {

  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[32]").appName("prepareData").getOrCreate()
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

    //cartella parquet inglesi
    val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(parquetFolder), "en")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    val startTime = System.currentTimeMillis()








    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}