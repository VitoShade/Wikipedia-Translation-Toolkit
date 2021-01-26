
import java.io._
import scala.collection.mutable
import org.apache.spark.sql._
import scalaj.http.Http

package Utilities {

  import API.{APILangLinks, APIPageView, APIRedirect}
  import org.apache.commons.io.FileUtils

  import java.net.URLDecoder
  import java.nio.charset.StandardCharsets

  object DataFrameUtility {

    def collectParquetFilesFromFolders(folders: Array[String]): Array[String] = {

      var allParquetFiles = Array[String]()

      folders.foreach(folderName => {

        val folder = new File(folderName)

        val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

        allParquetFiles = allParquetFiles ++ files

      })

      allParquetFiles
    }

    def collectParquetFromFoldersRecursively(folders: Array[String], subFolder: String): Array[String] = {

      var allParquetFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(2) == subFolder) {

          val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

          allParquetFiles = allParquetFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      allParquetFiles
    }

    def DEBUG_redirectDiRedirect(dataFrameSrc: DataFrame) {

      val redirect = dataFrameSrc.filter("id_redirect != ''").select("id_redirect").toDF("id2")

      dataFrameSrc.join(redirect, dataFrameSrc("id") === redirect("id2"), "inner").filter("id_redirect != ''").show(false)
    }

    def DEBUG_DDOS(): Unit = {

      var counter = 0

      (1 to 10000).foreach(_ => {

        println(counter)

        val response = Http("https://en.wikipedia.org/wiki/Natural_exponential_function").asString

        if(response.is2xx)
          counter += 1

      })
    }

    def collectErrorPagesFromFoldersRecursively(folders: Array[String], sparkSession: SparkSession, translated: Boolean): Dataset[String] = {

      var errorFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      val tr: String = if(translated) "Translated" else ""

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(5) == "error") {

          val files = folder.listFiles.filter(file => file.isFile && (
            file.toString.contains("errorLangLinks"+tr+".txt") || file.toString.contains("errorRedirect"+tr+".txt") || file.toString.contains("errorView"+tr+".txt")
            )).map(file => file.toString)

          errorFiles = errorFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val wikiPagesWithErrorRepetitions = errorFiles.map(x => {sparkSession.read.textFile(x)})

      val wikiPagesWithError = wikiPagesWithErrorRepetitions.reduce(_ union _).dropDuplicates()

      wikiPagesWithError
    }

    def DEBUG_collectSelectedErrorsFromFoldersRecursively(folders: Array[String], sparkSession: SparkSession, translated: Boolean, errorFileName: String): Dataset[String] = {

      var errorFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      val tr: String = if(translated) "Translated" else ""

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(5) == "error") {

          val files = folder.listFiles.filter(file => file.isFile && file.toString.contains(errorFileName + tr + ".txt")).map(file => file.toString)

          errorFiles = errorFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val wikiPagesWithErrorRepetitions = errorFiles.map(x => {sparkSession.read.textFile(x)})

      val wikiPagesWithError = wikiPagesWithErrorRepetitions.reduce(_ union _).dropDuplicates()

      wikiPagesWithError
    }

    def retryPagesWithErrorAndReplace(inputFolderName: String, outputFolderName: String, errorFolderName: String, folderSeparator: String, sparkSession: SparkSession): Unit = {

      //per convertire RDD in DataFrame
      import sparkSession.implicits._

      FileUtils.deleteDirectory(new File(outputFolderName))
      FileUtils.forceMkdir(new File(errorFolderName))

      //cartella parquet inglesi
      val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "en")

      val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

      //merge dei parquet in un dataFrame unico
      val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _).persist

      val errorPagesSrc = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession, false).toDF("id2").persist


      var counter = 0

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      val tempResultSrc = errorPagesSrc.map(page => {

        val line = page.getString(0)

        println(counter)

        //chiamata alle API per en.wikipedia e creazione di un record del DataFrame

        val tuple1 = APILangLinks.callAPI(line, "en", "it")

        val tuple2 = APIPageView.callAPI(line, "en")

        val tuple3 = APIRedirect.callAPI(line, "en")

        counter += 1

        (line, tuple1._1, URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      //creazione del DataFrame per en.wikipedia
      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val joinedDataFrameSrc = dataFrameSrc.join(errorPagesSrc, dataFrameSrc("id") === errorPagesSrc("id2"), "inner").
        select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameSrc = dataFrameSrc.except(joinedDataFrameSrc)

      val resultSrc = noErrorDataFrameSrc.union(tempDataFrameSrc)

      resultSrc.write.parquet(outputFolderName + folderSeparator + "en")

      //salvataggio degli errori per le API di en.wikipedia
      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinks.txt", APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorView.txt",      APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirect.txt",  APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksDetails.txt", APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewDetails.txt",      APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectDetails.txt",  APIRedirect.obtainErrorDetails())

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()





      //cartella parquet italiani
      val allInputFoldersDst = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "it")

      val dataFrameFilesDst = allInputFoldersDst map (tempFile => sparkSession.read.parquet(tempFile))

      //merge dei parquet in un dataFrame unico
      val dataFrameDst = dataFrameFilesDst.reduce(_ union _).persist

      val errorPagesDst = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession, true).toDF("id2").persist

      val dstPagesWithHash = errorPagesDst.filter(x => {x.getString(0).contains("#")})

      val dstPagesToRetryWithoutHash = errorPagesDst.except(dstPagesWithHash)

      val dstPagesToRetryWithHash = dstPagesWithHash.map(x => {

        //if(x.getString(0) != "") println(x)
        x.getString(0).split("#")(0)
      }).toDF("id2")

      val dstPagesToRetry = dstPagesToRetryWithoutHash.union(dstPagesToRetryWithHash)

      counter = 0

      val tempResultDst = dstPagesToRetry.map(page => {

        val line = page.getString(0)

        println(counter)

        //chiamata alle API per it.wikipedia e creazione di un record del DataFrame
        val tuple1 = APILangLinks.callAPI(line, "it", "en")

        val tuple2 = APIPageView.callAPI(line, "it")

        val tuple3 = APIRedirect.callAPI(line, "it")

        counter += 1

        (line, URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      }).persist

      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val joinedDataFrameDst = dataFrameDst.join(errorPagesDst, dataFrameDst("id") === errorPagesDst("id2"), "inner").
        select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameDst = dataFrameDst.except(joinedDataFrameDst)

      val resultDst = noErrorDataFrameDst.union(tempDataFrameDst)

      resultDst.write.parquet(outputFolderName + folderSeparator + "it")

      //salvataggio degli errori per le API di it.wikipedia
      this.writeFileID(errorFolderName + folderSeparator + "errorLangLinksTranslated.txt", APILangLinks.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorViewTranslated.txt",      APIPageView.obtainErrorID())
      this.writeFileID(errorFolderName + folderSeparator + "errorRedirectTranslated.txt",  APIRedirect.obtainErrorID())

      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksTranslatedDetails.txt", APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewTranslatedDetails.txt",      APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",  APIRedirect.obtainErrorDetails())

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
}