


import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.expressions.aggregate.Count

import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.{asc, desc}
import scala.collection.mutable.{WrappedArray => WA}


/*

object showData extends App {
  override def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().master("local[*]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val dataFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/tab_res"
    val inputFolder = new File(dataFolderName)
    val folderSeparator = "/"

    val tempOutputFolderSrc = Array(inputFolder + folderSeparator + "en" + folderSeparator)

    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFolderSrc)

    val dataFrameTempFilesDst = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    val dataF = dataFrameTempFilesDst.reduce(_ union _)

    //dataF.filter("(num_visualiz_anno[0] + num_visualiz_anno[1] + num_visualiz_anno[2]) > 100000").orderBy(desc("num_visualiz_anno")).show(numRows = 1000, truncate = false)

    val data1 =  dataF.select("num_visualiz_anno").rdd

    data1.take(1).foreach(row => println(row.getAs[WA[Long]](0).sum))



    sparkSession.stop()
  }
}

*/