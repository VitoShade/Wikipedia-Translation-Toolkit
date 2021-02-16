import Utilities.DataFrameUtility
import org.apache.spark.sql.SparkSession

object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    //val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    DataFrameUtility.dataFrameFromFoldersRecursively(Array("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati"), "en", sparkSession)
                    .coalesce(1)
                    .write
                    .parquet("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/nuovaCartella/en")
    DataFrameUtility.dataFrameFromFoldersRecursively(Array("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati"), "it", sparkSession)
                    .coalesce(1)
                    .write
                    .parquet("/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/nuovaCartella/it")

    sparkSession.stop()
  }

  def memoryInfo(): Unit = {
    val mb = 1024*1024
    val runtime = Runtime.getRuntime
    println("ALL RESULTS IN MB")
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
  }

}