
import org.apache.spark.sql.SparkSession
import API.{APIPageView, _}
import Utilities._

object downloadData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[20]").appName("downloadData").getOrCreate()
    //val sparkSession = SparkSession.builder().appName("downloadData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName   = "indici\\"
    val tempFolderName    = "tempResult\\"
    val folderSeparator   = "\\"

    val startTime = System.currentTimeMillis()

    //raccolta di tutti i file .txt nella cartella di input
    val path = args(0)
    val inputFiles = args.drop(1)

    //ciclo sui file nella cartella di input
    inputFiles.foreach(inputFileName => {

      //caricamento del file di input e divisione in task
      val input = sparkContext.textFile(path + inputFolderName + inputFileName, 60)

      //chiamata alle API per en.wikipedia e creazione di un record del DataFrame
      val tempDataFrameSrc = input.map(line => {
        val tuple1 = APILangLinks.callAPI(line, "en", "it")
        val tuple2 = APIPageView.callAPI(line, "en")
        val tuple3 = APIRedirect.callAPI(line, "en")
        (line, tuple1._1, tuple1._2, tuple2._1, tuple2._2, tuple3._1, tuple3._2)
      }).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect").persist

      //creazione di una cartella temporanea per il file appena processato
      val tempOutputName = inputFileName.dropRight(4)

      //             "s3n://wtt-s3-X/" "tempResult/"       "fileXXX"           "/"         "en"
      val tempOutputFolderSrc = path + tempFolderName + tempOutputName + folderSeparator + "en"

      //per chiamare la persist ed eseguire i download in parallelo
      tempDataFrameSrc.count()

      //salvataggio del file temporaneo
      tempDataFrameSrc.coalesce(1).write.parquet(tempOutputFolderSrc)

      val errors = APILangLinks.obtainErrorID() ++ APIPageView.obtainErrorID() ++ APIRedirect.obtainErrorID()

      //salvataggio degli errori per le API di en.wikipedia
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "errors", errors, sparkContext)

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      //recupero colonna ID pagine tradotte
      val dataFrameTranslatedID = tempDataFrameSrc.filter("id_pagina_tradotta != ''").select("id_pagina_tradotta")

      //recupero ID pagine tradotte
      val translatedID = dataFrameTranslatedID.map(row => row.getString(0))

      //chiamata alle API per it.wikipedia e creazione di un record del DataFrame
      val tempDataFrameDst = translatedID.map(line => {
        val tuple1 = APILangLinks.callAPI(line, "it", "en")
        val tuple2 = APIPageView.callAPI(line, "it")
        val tuple3 = APIRedirect.callAPI(line, "it")
        (line, tuple1._2, tuple2._1, tuple2._2, tuple3._1, tuple3._2)
      }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect").persist

      //             "s3n://wtt-s3-X/" "tempResult/"       "fileXXX"           "/"         "it"
      val tempOutputFolderDst = path + tempFolderName + tempOutputName + folderSeparator + "it"

      //per chiamare la persist ed eseguire i download in parallelo
      tempDataFrameDst.count()

      //salvataggio del file temporaneo
      tempDataFrameDst.coalesce(1).write.parquet(tempOutputFolderDst)

      val errorsTranslated = APILangLinks.obtainErrorID() ++ APIPageView.obtainErrorID() ++ APIRedirect.obtainErrorID()

      //salvataggio degli errori per le API di it.wikipedia
      DataFrameUtility.writeFileID(path + tempFolderName + tempOutputName + folderSeparator + "errorsTranslated", errorsTranslated, sparkContext)

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()
    })

    val endTime = System.currentTimeMillis()

    println("Time: " + (endTime - startTime) / 60000 + " minutes " + ((endTime - startTime) / 1000) % 60 + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
