


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
    val inputFolderName2 = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati\\File1-3"
    val folderSeparator = "\\"

    val errorPages = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession, false).toDF("id2")


    val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName2), "en")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    val joinedDataFrame = dataFrameSrc.join(errorPages, dataFrameSrc("id") === errorPages("id2"), "inner").
      select("id", "num_traduzioni", "id_pagina_tradotta","num_visualiz_anno","num_visualiz_mesi","byte_dim_page","id_redirect")

    dataFrameSrc.filter("id == 'Natpisit_Chompoonuch'").show(false)

    val resultDataFrame = dataFrameSrc.except(joinedDataFrame)

    resultDataFrame.filter("id == 'Natpisit_Chompoonuch'").show(false)




    //val temp =

    //res.foreach(println)

    /*val tempOutputFolderSrc = Array(inputFolder + folderSeparator + "en" + folderSeparator)

    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFolderSrc)

    val dataFrameTempFilesDst = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    val dataF = dataFrameTempFilesDst.reduce(_ union _)

    dataF.filter("(num_visualiz_anno[0] + num_visualiz_anno[1] + num_visualiz_anno[2]) > 100000").orderBy(desc("num_visualiz_anno")).show(numRows = 1000, truncate = false)*/


    /*var counter = 0

    (1 to 10000).foreach(_ => {

      println(counter)

      val response = Http("https://en.wikipedia.org/wiki/Natural_exponential_function").asString

      if(response.is2xx)
        counter += 1

    })*/


    sparkSession.stop()
  }
}