


import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.desc




object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")


    val inputFolderName = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati"
    val folderSeparator = "/"

    val startTime = System.currentTimeMillis()

    DataFrameUtility.DEBUG_newDataFrame(Array(inputFolderName), sparkSession)



    sparkSession.stop()
  }
}