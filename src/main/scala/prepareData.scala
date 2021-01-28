
import org.apache.spark.sql.SparkSession
import API._
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[20]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val startTime = System.currentTimeMillis()
    val inputFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/toRun"
    val tempFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/tmp"
    val outputFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/output"
    val errorFolderName   = "/Users/marco/OfflineDocs/Wikipedia_Dump/output/error"
    val folderSeparator = "/"

    val inputFolder = new File(inputFolderName)

    //raccolta di tutti i file .txt nella cartella di input
    val inputFiles = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)

    var tempOutputFoldersSrc = Array[String]()
    var tempOutputFoldersDst = Array[String]()

    FileUtils.deleteDirectory(new File(tempFolderName))
    FileUtils.deleteDirectory(new File(outputFolderName))
    FileUtils.forceMkdir(new File(errorFolderName))

    //ciclo sui file nella cartella di input
    inputFiles.foreach(inputFileName => {

      //caricamento del file di input e divisione in task
      val input = sparkContext.textFile(inputFileName, 60)

      var counter = 0

      //chiamata alle API per en.wikipedia e creazione di un record del DataFrame
      val tempResultSrc = input.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "en", "it")

        val tuple2 = APIPageView.callAPI(line, "en")

        val tuple3 = APIRedirect.callAPI(line, "en")

        counter += 1

        (line, tuple1._1, URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      //creazione del DataFrame per en.wikipedia
      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //creazione di una cartella temporanea per il file appena processato
      val tempOutputName = inputFileName.drop(inputFolderName.length + 1).dropRight(4)

      val tempOutputFolderSrc = tempFolderName + folderSeparator + "en" + folderSeparator + tempOutputName

      //salvataggio del nome della cartella temporanea
      tempOutputFoldersSrc = tempOutputFoldersSrc :+ tempOutputFolderSrc

      //salvataggio del file temporaneo
      tempDataFrameSrc.write.parquet(tempOutputFolderSrc)

      //salvataggio degli errori per le API di en.wikipedia
      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinks.txt",    APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorView.txt", APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirect.txt",     APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksDetails.txt",     APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewDetails.txt",  APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectDetails.txt",      APIRedirect.obtainErrorDetails())

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      //recupero colonna ID pagine tradotte
      val dataFrameTranslatedID = tempDataFrameSrc.filter("id_pagina_tradotta != ''").select("id_pagina_tradotta")

      //recupero ID pagine tradotte
      val translatedID = dataFrameTranslatedID.map(row => row.getString(0))

      counter = 0

      //chiamata alle API per it.wikipedia e creazione di un record del DataFrame
      val tempResultDst = translatedID.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "it", "en")

        val tuple2 = APIPageView.callAPI(line, "it")

        val tuple3 = APIRedirect.callAPI(line, "it")

        counter += 1

        (line, URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      //creazione del DataFrame per it.wikipedia
      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val tempOutputFolderDst = tempFolderName + folderSeparator + "it" + folderSeparator + tempOutputName

      //salvataggio del nome della cartella temporanea
      tempOutputFoldersDst = tempOutputFoldersDst :+ tempOutputFolderDst

      //salvataggio del file temporaneo
      tempDataFrameDst.write.parquet(tempOutputFolderDst)

      //salvataggio degli errori per le API di it.wikipedia
      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinksTranslated.txt",    APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorViewTranslated.txt", APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirectTranslated.txt",     APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksTranslatedDetails.txt",     APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewTranslatedDetails.txt",  APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",      APIRedirect.obtainErrorDetails())

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

    })

    //recupero del nome dei file parquet temporanei per en.wikipedia
    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersSrc)

    //caricamento dei file parquet temporanei
    val dataFrameTempFilesSrc = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //unione dei DataFrame temporanei in un unico DataFrame
    val notCompressedDataFrameSrc = dataFrameTempFilesSrc.reduce(_ union _)

    notCompressedDataFrameSrc.show(false)

    //calcolo del numero di file di output(numero di partizioni)
    var numPartitionsSrc = tempOutputFoldersSrc.length / 2

    if(numPartitionsSrc < 1)
      numPartitionsSrc = 1

    //una partizione ogni 2 file di input
    val resultDataFrameSrc = notCompressedDataFrameSrc.coalesce(numPartitionsSrc)

    resultDataFrameSrc.write.parquet(outputFolderName + folderSeparator + "en")

    //recupero del nome dei file parquet temporanei per en.wikipedia
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

  def writeFileID(filePath: String, listID: Vector[String]): Unit = {
    val file = new File(filePath)
    if(!file.exists) file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file, true))
    listID.foreach( id => bw.write(id + "\n"))
    bw.close()
  }

  def writeFileErrors(filePath: String, listErrors: Vector[(String, Vector[(Int, String)])]): Unit = {
    val file = new File(filePath)
    if(!file.exists) file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file, true))
    listErrors.foreach( ErrorTuple => bw.write(ErrorTuple._1 + ": " + ErrorTuple._2.map(tuple => "(" + tuple._1 + " , " + tuple._2  + ")" + "\t") + "\n"))
    bw.close()
  }
}
