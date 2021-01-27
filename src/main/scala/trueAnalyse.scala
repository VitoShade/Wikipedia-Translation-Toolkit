import Utilities._
import org.apache.spark.sql.SparkSession

object trueAnalyse extends App {

  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[32]").appName("trueAnalyse").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali"
    val errorFolderName  = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\error"
    val sizeFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\size"
    val folderSeparator   = "\\"

    //dataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(inputFolderName), "en", sparkSession)


    val startTime = System.currentTimeMillis()








    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}