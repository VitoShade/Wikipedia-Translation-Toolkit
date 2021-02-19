import Utilities.DataFrameUtility
import org.apache.spark.sql.SparkSession

object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    //val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    DataFrameUtility.dataFrameFromFoldersRecursively(Array("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati/File1-10/"), "en", sparkSession)
                    .coalesce(1)
                    .write
                    .parquet("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/nuovaCartella1-10/en")
    DataFrameUtility.dataFrameFromFoldersRecursively(Array("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati/File1-10/"), "it", sparkSession)
                    .coalesce(1)
                    .write
                    .parquet("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/nuovaCartella1-10/it")

    sparkSession.stop()
  }
}