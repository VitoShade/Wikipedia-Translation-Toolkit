
import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

//case class perché sono immutabili
case class EntrySrc(id: String,
                 numTraduzioni: Int,
                 IDPaginaTradotta: String,
                 numVisualizzazioniAnno: List[Int],
                 numVisualizzazioniMesi: List[Int],
                 numByte: Int,
                 IDPaginaPrincipale: String)

case class EntryDst(id: String,
                    IDPaginaOriginale: String,
                    numVisualizzazioniAnno: List[Int],
                    numVisualizzazioniMesi: List[Int],
                    numByte: Int,
                    IDPaginaPrincipale: String)

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[20]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\indici"
    val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val folderSeparator = "\\"

    val inputFolder = new File(inputFolderName)

    val inputFiles = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)

    var tempOutputFoldersSrc = Array[String]()
    var tempOutputFoldersDst = Array[String]()

    //inputFiles.foreach(println)
    inputFiles.foreach(inputFileName => {

      val input = sparkContext.textFile(inputFileName, 32)

      var counter = 0

      val tempResultSrc = input.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "en", "it")
        val num_traduzioni = tuple1._1
        val id_pagina_tradotta = tuple1._2
        //println(tuple1)

        val tuple2 = APIPageView.callAPI(line, "en")
        val num_visualiz_anno = tuple2._1
        val num_visualiz_mesi = tuple2._2
        //println(line + "   " + tuple2)
        //println(num_visualiz_anno)

        val tuple3 = APIRedirect.callAPI(line, "en")
        val byte_dim_page = tuple3._1
        val id_redirect = tuple3._2
        //println(result3)

        counter += 1

        EntrySrc(line, num_traduzioni, id_pagina_tradotta, num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)

      }).persist

      //println(result)

      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //tempDataFrame.show(false)

      val tempOutputName = inputFileName.drop(inputFolderName.length + 1).dropRight(4)

      val tempOutputFolderSrc = tempFolderName + folderSeparator + "en" + folderSeparator + tempOutputName

      tempOutputFoldersSrc = tempOutputFoldersSrc :+ tempOutputFolderSrc

      FileUtils.deleteDirectory(new File(tempOutputFolderSrc))
      tempDataFrameSrc.write.parquet(tempOutputFolderSrc)

      //recupero colonna ID pagine tradotte
      val dataFrameTranslatedID = tempDataFrameSrc.filter("id_pagina_tradotta != ''").select("id_pagina_tradotta")

      //dataFrameTranslatedID.show(false)

      //recupero ID pagine tradotte
      val translatedID = dataFrameTranslatedID.map(row => row.getString(0))

      counter = 0

      val tempResultDst = translatedID.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(URLDecoder.decode(line,  StandardCharsets.UTF_8), "it", "en")
        //val num_traduzioni = tuple1._1
        val id_pagina_originale = tuple1._2
        //println(tuple1)

        val tuple2 = APIPageView.callAPI(URLDecoder.decode(line,  StandardCharsets.UTF_8), "it")
        val num_visualiz_anno = tuple2._1
        val num_visualiz_mesi = tuple2._2

        val tuple3 = APIRedirect.callAPI(URLDecoder.decode(line,  StandardCharsets.UTF_8), "it")
        val byte_dim_page = tuple3._1
        val id_redirect = tuple3._2

        counter += 1

        EntryDst(URLDecoder.decode(line,  StandardCharsets.UTF_8), URLDecoder.decode(id_pagina_originale,  StandardCharsets.UTF_8), num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)

      }).persist

      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val tempOutputFolderDst = tempFolderName + folderSeparator + "it" + folderSeparator + tempOutputName

      tempOutputFoldersDst = tempOutputFoldersDst :+ tempOutputFolderDst

      FileUtils.deleteDirectory(new File(tempOutputFolderDst))
      tempDataFrameDst.write.parquet(tempOutputFolderDst)

      println("Lista errori APILangLinks: " + APILangLinks.lista_errori)
      println("Lista errori APIPageView:  " + APIPageView.lista_errori)
      println("Lista errori APIRedirect:  " + APIRedirect.lista_errori)

    })

    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersSrc)

    val dataFrameTempFilesSrc = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrameSrc = dataFrameTempFilesSrc.reduce(_ union _)

    notCompressedDataFrameSrc.show(false)

    val resultDataFrameSrc = notCompressedDataFrameSrc.coalesce(2)

    FileUtils.deleteDirectory(new File(outputFolderName + folderSeparator + "en"))
    resultDataFrameSrc.write.parquet(outputFolderName + folderSeparator + "en")


    val allTempFilesDst = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersDst)

    val dataFrameTempFilesDst = allTempFilesDst map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrameDst = dataFrameTempFilesDst.reduce(_ union _)

    notCompressedDataFrameDst.show(false)

    val resultDataFrameDst = notCompressedDataFrameDst.coalesce(2)

    FileUtils.deleteDirectory(new File(outputFolderName + folderSeparator + "it"))
    resultDataFrameDst.write.parquet(outputFolderName + folderSeparator + "it")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
