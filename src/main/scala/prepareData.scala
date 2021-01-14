
import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._

//case class perché sono immutabili
case class Entry(id: String,
                 numTraduzioni: Int,
                 IDPaginaTradotta: String,
                 numVisualizzazioniAnno: List[Int],
                 numVisualizzazioniMesi: List[Int],
                 numByte: Int,
                 IDPaginaPrincipale: String)

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    //TODO: cambiare path
    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\indici"
    val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val folderSeparator = "\\"

    val inputFolder = new File(inputFolderName)

    val inputFiles: Array[String] = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)

    var tempOutputFolders: Array[String] = Array[String]()

    //inputFiles.foreach(println)
    inputFiles.foreach(inputFileName => {

      val input: org.apache.spark.rdd.RDD[String] = sparkContext.textFile(inputFileName)

      val tempResult = input.map(line => {

        //println(line)

        var tuple1 = APILangLinks.callAPI(line, "en", "it")
        val num_traduzioni = tuple1._1
        val id_pagina_tradotta = tuple1._2
        //println(tuple1)

        var tuple2 = APIPageView.callAPI(line, "en")
        val num_visualiz_anno = tuple2._1
        val num_visualiz_mesi = tuple2._2
        //println(line + "   " + tuple2)
        //println(num_visualiz_anno)

        var tuple3 = APIRedirect.callAPI(line, "en")
        val byte_dim_page = tuple3._1
        val id_redirect = tuple3._2
        //println(result3)

        Entry(line, num_traduzioni, id_pagina_tradotta, num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)
      }).persist

      //println(result)

      val tempDataFrame = tempResult.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //tempDataFrame.show(false)

      val tempOutputName = inputFileName.drop(inputFolderName.length + 1).dropRight(4)

      val tempOutputFolder = tempFolderName + folderSeparator + tempOutputName

      tempOutputFolders = tempOutputFolders :+ tempOutputFolder

      FileUtils.deleteDirectory(new File(tempOutputFolder))
      tempDataFrame.write.parquet(tempOutputFolder)

    })

    val allTempFiles = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFolders)

    val dataFrameTempFiles = allTempFiles map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrame = dataFrameTempFiles.reduce(_ union _)

    notCompressedDataFrame.show(false)

    //notCompressedDataFrame.select("id_pagina_tradotta").show(false)

    val col = notCompressedDataFrame.filter("id_pagina_tradotta != ''").select("id_pagina_tradotta")

    col.show(false)

    val translatedIndexes = col.map(row => row.getString(0))

    translatedIndexes.foreach(n => println(n))

    //col.(n => println(n))

    //tempRes.foreach(n => println(n))

    val resultDataFrame = notCompressedDataFrame.coalesce(2)

    FileUtils.deleteDirectory(new File(outputFolderName))
    resultDataFrame.write.parquet(outputFolderName)

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
