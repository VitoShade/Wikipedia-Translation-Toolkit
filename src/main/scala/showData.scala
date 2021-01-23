


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


    val sparkSession = SparkSession.builder().master("local[30]").appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val dataFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/finiti_copy"
    val inputFolder = new File(dataFolderName)
    val folderSeparator = "/"

    /*val tempOutputFolderSrc = Array(inputFolder + folderSeparator + "en" + folderSeparator)

    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFolderSrc)

    val dataFrameTempFilesDst = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    val dataF = dataFrameTempFilesDst.reduce(_ union _)

    dataF.filter("(num_visualiz_anno[0] + num_visualiz_anno[1] + num_visualiz_anno[2]) > 100000").orderBy(desc("num_visualiz_anno")).show(numRows = 1000, truncate = false)*/

    var counter = 0

    (1 to 10000).foreach(_ => {

      println(counter)

      val response = Http("https://en.wikipedia.org/wiki/Natural_exponential_function").asString

      if(response.is2xx)
        counter += 1

    })


    sparkSession.stop()
  }
}