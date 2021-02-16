
import org.apache.spark.sql.SparkSession
import API._
import Utilities._
import java.net._

object downloadData extends App {
  override def main(args: Array[String]) {

    //val sparkSession = SparkSession.builder().master("local[20]").appName("downloadData").getOrCreate()
    val sparkSession = SparkSession.builder().appName("downloadData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    //println("Working with " + DataFrameUtility.numPartitions + " partitions")

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName   = "indici/"
    val tempFolderName    = "tempResult/"
    val folderSeparator   = "/"

    val startTime = System.currentTimeMillis()

    //val inputFolder = new File(inputFolderName)

    //raccolta di tutti i file .txt nella cartella di input
    //val inputFiles = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)
    val path = args(0)
    val inputFiles = args.drop(1)



    //var tempOutputFoldersSrc = Array[String]()
    //var tempOutputFoldersDst = Array[String]()

    /*
    FileUtils.deleteDirectory(new File(tempFolderName))
    FileUtils.deleteDirectory(new File(outputFolderName))
    FileUtils.forceMkdir(new File(errorFolderName))
    */

    //ciclo sui file nella cartella di input
    inputFiles.foreach(inputFileName => {

      //caricamento del file di input e divisione in task
      val input = sparkContext.textFile(path + inputFolderName + inputFileName, 60)

      var counter = 0

      //chiamata alle API per en.wikipedia e creazione di un record del DataFrame
      val tempResultSrc = input.map(line => {

        println(counter)

        val tuple1 = APILangLinks.callAPI(line, "en", "it")

        val tuple2 = APIPageView.callAPI(line, "en")

        val tuple3 = APIRedirect.callAPI(line, "en")

        counter += 1

        (line, tuple1._1, URLDecoder.decode(tuple1._2,  "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      //creazione del DataFrame per en.wikipedia
      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //creazione di una cartella temporanea per il file appena processato
      val tempOutputName = inputFileName.dropRight(4)

      //             "s3n://wtt-s3-1/" "tempResult/"      "fileXX"            "/"          "en"
      val tempOutputFolderSrc = path + tempFolderName + tempOutputName + folderSeparator + "en"

      //salvataggio del nome della cartella temporanea
      //tempOutputFoldersSrc = tempOutputFoldersSrc :+ tempOutputFolderSrc

      //per chiamare la persist
      tempDataFrameSrc.count()

      //salvataggio del file temporaneo
      tempDataFrameSrc.coalesce(1).write.parquet(tempOutputFolderSrc)

      //salvataggio degli errori per le API di en.wikipedia
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorLangLinks", APILangLinks.obtainErrorID(), sparkContext)
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorView",      APIPageView.obtainErrorID(), sparkContext)
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorRedirect",  APIRedirect.obtainErrorID(), sparkContext)

      /*
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorLangLinksDetails.txt", APILangLinks.obtainErrorDetails())
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorViewDetails.txt",      APIPageView.obtainErrorDetails())
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorRedirectDetails.txt",  APIRedirect.obtainErrorDetails())
      */

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

        (line, URLDecoder.decode(tuple1._2,  "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      //creazione del DataFrame per it.wikipedia
      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //val tempOutputFolderDst = tempFolderName + folderSeparator + "it" + folderSeparator + tempOutputName
      //             "s3n://wtt-s3-1/" "tempResult/"      "fileXX"            "/"          "it"
      val tempOutputFolderDst = path + tempFolderName + tempOutputName + folderSeparator + "it"

      //salvataggio del nome della cartella temporanea
      //tempOutputFoldersDst = tempOutputFoldersDst :+ tempOutputFolderDst

      //per chiamare la persist
      tempDataFrameDst.count()

      //salvataggio del file temporaneo
      tempDataFrameDst.coalesce(1).write.parquet(tempOutputFolderDst)

      //salvataggio degli errori per le API di it.wikipedia
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorLangLinksTranslated", APILangLinks.obtainErrorID(), sparkContext)
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorViewTranslated",      APIPageView.obtainErrorID(), sparkContext)
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "error" + folderSeparator + "errorRedirectTranslated",  APIRedirect.obtainErrorID(), sparkContext)

      /*
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorLangLinksTranslatedDetails.txt", APILangLinks.obtainErrorDetails())
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorViewTranslatedDetails.txt",      APIPageView.obtainErrorDetails())
      DataFrameUtility.writeFileErrors(tempFolderName + folderSeparator + tempOutputName + folderSeparator + "error" + folderSeparator + "errorRedirectTranslatedDetails.txt",  APIRedirect.obtainErrorDetails())
      */

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

    })


    //questa parte si potrebbe fare dopo
    /*
    //recupero del nome dei file parquet temporanei per en.wikipedia
    val allTempFilesSrc = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersSrc)

    //caricamento dei file parquet temporanei
    val dataFrameTempFilesSrc = allTempFilesSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //unione dei DataFrame temporanei in un unico DataFrame
    val notCompressedDataFrameSrc = dataFrameTempFilesSrc.reduce(_ union _)

    notCompressedDataFrameSrc.show(false)


    //una partizione ogni 2 file di input
    val resultDataFrameSrc = notCompressedDataFrameSrc.coalesce(1)

    resultDataFrameSrc.write.parquet(outputFolderName + folderSeparator + "en")

    //recupero del nome dei file parquet temporanei per en.wikipedia
    val allTempFilesDst = DataFrameUtility.collectParquetFilesFromFolders(tempOutputFoldersDst)

    val dataFrameTempFilesDst = allTempFilesDst map (tempFile => sparkSession.read.parquet(tempFile))

    val notCompressedDataFrameDst = dataFrameTempFilesDst.reduce(_ union _)

    notCompressedDataFrameDst.show(false)

    //un quarto delle partizioni rispetto alla lingua di partenza
    val resultDataFrameDst = notCompressedDataFrameDst.coalesce(1)

    resultDataFrameDst.write.parquet(outputFolderName + folderSeparator + "it")
     */


    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
