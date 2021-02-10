import org.apache.spark.sql.SparkSession

object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    (0 to 3).map(j => {})


    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\error"
    val folderSeparator = "\\"

    val startTime = System.currentTimeMillis()

    //DataFrameUtility.DEBUG_newDataFrame(Array(inputFolderName), sparkSession)

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}