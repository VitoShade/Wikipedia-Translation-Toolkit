


import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils

import java.io._
import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.desc
import scalaj.http.Http




object showData extends App {
  override def main(args: Array[String]) {


    val sparkSession = SparkSession.builder().master("local[2]").appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati"
    val folderSeparator = "\\"

    val res = DataFrameUtility.DEBUG_collectSelectedErrorsFromFoldersRecursively(Array(inputFolderName), sparkSession, false, "errorView")


    /*val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "it")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)


    println(dataFrameSrc.filter("id_redirect != ''").count())

    dataFrameSrc.filter("id_redirect != ''").show(50, false)*/



    sparkSession.stop()
  }
}