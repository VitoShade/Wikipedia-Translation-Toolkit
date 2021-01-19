
import org.apache.spark.sql.SparkSession
import API._
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

//case class perché sono immutabili
case class EntrySrc(id: String,
                 numTraduzioni: Int,
                 IDPaginaTradotta: String,
                 numVisualizzazioniAnno: Array[Int],
                 numVisualizzazioniMesi: Array[Int],
                 numByte: Int,
                 IDPaginaPrincipale: String)

case class EntryDst(id: String,
                    IDPaginaOriginale: String,
                    numVisualizzazioniAnno: Array[Int],
                    numVisualizzazioniMesi: Array[Int],
                    numByte: Int,
                    IDPaginaPrincipale: String)

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[25]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\indici"
    val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result\\error"
    val folderSeparator = "\\"

    val inputFolder = new File(inputFolderName)

    val inputFiles = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)

    var tempOutputFoldersSrc = Array[String]()
    var tempOutputFoldersDst = Array[String]()

    FileUtils.deleteDirectory(new File(tempFolderName))
    FileUtils.deleteDirectory(new File(outputFolderName))
    FileUtils.forceMkdir(new File(errorFolderName))

    //inputFiles.foreach(println)
    inputFiles.foreach(inputFileName => {

      val input = sparkContext.textFile(inputFileName, 60)

      var counter = 0

      val tempResultSrc = input.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "en", "it")
        val num_traduzioni = tuple1._1
        val id_pagina_tradotta = URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8)
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

      tempDataFrameSrc.write.parquet(tempOutputFolderSrc)

      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinks.txt",    APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorViewInstance.txt", APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirect.txt",     APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksDetails.txt",     APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewInstanceDetails.txt",  APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectDetails.txt",      APIRedirect.obtainErrorDetails())

      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      //recupero colonna ID pagine tradotte
      val dataFrameTranslatedID = tempDataFrameSrc.filter("id_pagina_tradotta != ''").select("id_pagina_tradotta")

      //dataFrameTranslatedID.show(false)

      //recupero ID pagine tradotte
      val translatedID = dataFrameTranslatedID.map(row => row.getString(0))

      counter = 0

      val tempResultDst = translatedID.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "it", "en")
        //val num_traduzioni = tuple1._1
        val id_pagina_originale = URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8)
        //println(tuple1)

        val tuple2 = APIPageView.callAPI(line, "it")
        val num_visualiz_anno = tuple2._1
        val num_visualiz_mesi = tuple2._2

        val tuple3 = APIRedirect.callAPI(line, "it")
        val byte_dim_page = tuple3._1
        val id_redirect = tuple3._2

        counter += 1

        EntryDst(line, id_pagina_originale, num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)

      }).persist

      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val tempOutputFolderDst = tempFolderName + folderSeparator + "it" + folderSeparator + tempOutputName

      tempOutputFoldersDst = tempOutputFoldersDst :+ tempOutputFolderDst

      tempDataFrameDst.write.parquet(tempOutputFolderDst)

      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinksTranslated.txt",    APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorViewInstanceTranslated.txt", APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirectTranslated.txt",     APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksTranslatedDetails.txt",     APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewInstanceTranslatedDetails.txt",  APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",      APIRedirect.obtainErrorDetails())

      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()
    })

    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersSrc)

    val dataFrameTempFilesSrc = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrameSrc = dataFrameTempFilesSrc.reduce(_ union _)

    notCompressedDataFrameSrc.show(false)

    var numPartitionsSrc = tempOutputFoldersSrc.length / 2

    if(numPartitionsSrc < 1)
      numPartitionsSrc = 1

    //una partizione ogni 2 file di input
    val resultDataFrameSrc = notCompressedDataFrameSrc.coalesce(numPartitionsSrc)

    resultDataFrameSrc.write.parquet(outputFolderName + folderSeparator + "en")


    val allTempFilesDst = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersDst)

    val dataFrameTempFilesDst = allTempFilesDst map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrameDst = dataFrameTempFilesDst.reduce(_ union _)

    notCompressedDataFrameDst.show(false)

    var numPartitionsDst = numPartitionsSrc / 4

    if(numPartitionsDst < 1)
      numPartitionsDst = 1

    //un quarto delle partizioni rispetto alla lingua di partenza
    val resultDataFrameDst = notCompressedDataFrameDst.coalesce(numPartitionsDst)

    resultDataFrameDst.write.parquet(outputFolderName + folderSeparator + "it")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

  def writeFileID(filePath:String, listID:Vector[String]): Unit = {
    val file = new File(filePath)
    if(!file.exists) file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file, true))
    listID.foreach( id => bw.write(id + "\n"))
    bw.close()
  }

  def writeFileErrors(filePath:String, listErrors:Vector[(String, Vector[(Int, String)])]): Unit = {
    val file = new File(filePath)
    if(!file.exists) file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file, true))
    listErrors.foreach( ErrorTuple => bw.write(ErrorTuple._1 + ": " + ErrorTuple._2.map(tuple => "(" + tuple._1 + " , " + tuple._2  + ")" + "\t") + "\n"))
    bw.close()
  }
}
