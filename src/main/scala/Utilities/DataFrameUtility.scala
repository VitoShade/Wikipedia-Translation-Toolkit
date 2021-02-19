
import java.io._
import scala.collection.mutable
import org.apache.spark.sql._
import scalaj.http.Http
import API.{APILangLinks, APIPageView, APIRedirect}
import org.apache.commons.io.FileUtils
import java.net._
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

package Utilities {

  object DataFrameUtility {

    var numPartitions = 8

    def collectParquetFilesFromFolders(folders: Array[String]): Array[String] = {

      var allParquetFiles = Array[String]()

      folders.foreach(folderName => {

        val folder = new File(folderName)

        val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

        allParquetFiles = allParquetFiles ++ files

      })

      allParquetFiles
    }

    def dataFrameFromFoldersRecursively(folders: Array[String], subFolder: String, sparkSession: SparkSession): DataFrame = {

      var allParquetFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(subFolder.length) == subFolder) {

          val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

          allParquetFiles = allParquetFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val dataFrameFiles = allParquetFiles map (tempFile => sparkSession.read.parquet(tempFile))

      //merge dei parquet in un dataFrame unico
      dataFrameFiles.reduce(_ union _)
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

    def collectErrorPagesFromFoldersRecursively(errorFiles: Array[String], sparkSession: SparkSession): Dataset[String] = {
      /*
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


       */
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

    def retryPagesWithErrorAndReplace(dataFrameSrc: DataFrame, dataFrameDst: DataFrame, errorPagesSrc: DataFrame, errorPagesDst: DataFrame, sparkSession: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame) = {

      //val sparkContext = sparkSession.sparkContext

      //per convertire RDD in DataFrame
      import sparkSession.implicits._

      /*
      FileUtils.deleteDirectory(new File(outputFolderName))
      FileUtils.forceMkdir(new File(errorFolderName))
      */

      //dataFrame dai parquet inglesi
      //val dataFrameSrc = this.dataFrameFromFoldersRecursively(Array(inputFolderName), "en", sparkSession)

      //val errorPagesSrc = this.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession).toDF("id2")
      //val errorPagesSrc = sparkSession.read.textFile(errorFolderName + errorSrcFile).toDF("id2")

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

        (line, tuple1._1, URLDecoder.decode(tuple1._2, "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      })//.repartition(numPartitions)

      //creazione del DataFrame per en.wikipedia
      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      tempDataFrameSrc.persist

      val joinedDataFrameSrc = dataFrameSrc.join(errorPagesSrc, dataFrameSrc("id") === errorPagesSrc("id2"), "inner").
        select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameSrc = dataFrameSrc.except(joinedDataFrameSrc)//.repartition(numPartitions)

      val resultSrc = noErrorDataFrameSrc.union(tempDataFrameSrc).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //resultSrc.coalesce(1).write.parquet(outputFolderName + "/" + "en")

      //salvataggio degli errori per le API di en.wikipedia
      /*
      this.writeFileID(errorFolderName + "/" + "errorLangLinks.txt", APILangLinks.obtainErrorID(), sparkContext)
      this.writeFileID(errorFolderName + "/" + "errorView.txt",      APIPageView.obtainErrorID(), sparkContext)
      this.writeFileID(errorFolderName + "/" + "errorRedirect.txt",  APIRedirect.obtainErrorID(), sparkContext)
      */

      val errorSrcDuplicates = Array[DataFrame](APILangLinks.obtainErrorID().toDF("id2"), APIPageView.obtainErrorID().toDF("id2"), APIRedirect.obtainErrorID().toDF("id2"))

      val errorSrc = errorSrcDuplicates.reduce(_ union _).dropDuplicates().toDF("id2")

      /*
      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksDetails.txt", APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewDetails.txt",      APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectDetails.txt",  APIRedirect.obtainErrorDetails())
      */

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      //pagine italiane da scaricare perché trovate con il retry della pagina inglese
      val newDstPages = tempDataFrameSrc.select("id_pagina_tradotta").filter("id_pagina_tradotta != ''").toDF("id2")

      //dataFrame dei parquet italiani
      //val dataFrameDst = this.dataFrameFromFoldersRecursively(Array(inputFolderName), "it", sparkSession)

      //var errorPagesDst = this.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession).toDF("id2")

      val errorPagesDst2 = errorPagesDst.union(newDstPages)

      val dstPagesWithHash = errorPagesDst2.filter(x => {x.getString(0).contains("#")})

      val dstPagesToRetryWithoutHash = errorPagesDst2.except(dstPagesWithHash)

      val dstPagesToRetryWithHash = dstPagesWithHash.map(x => {
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

        (line, URLDecoder.decode(tuple1._2, "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      })//.repartition(numPartitions)

      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val joinedDataFrameDst = dataFrameDst.join(errorPagesDst2, dataFrameDst("id") === errorPagesDst2("id2"), "inner").
        select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameDst = dataFrameDst.except(joinedDataFrameDst)//.repartition(numPartitions)

      val resultDst = noErrorDataFrameDst.union(tempDataFrameDst).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //resultDst.coalesce(1).write.parquet(outputFolderName + "/" + "it")

      //salvataggio degli errori per le API di it.wikipedia
      /*
      this.writeFileID(errorFolderName + "/" + "errorLangLinksTranslated.txt", APILangLinks.obtainErrorID(), sparkContext)
      this.writeFileID(errorFolderName + "/" + "errorViewTranslated.txt",      APIPageView.obtainErrorID(), sparkContext)
      this.writeFileID(errorFolderName + "/" + "errorRedirectTranslated.txt",  APIRedirect.obtainErrorID(), sparkContext)
      */

      val errorDstDuplicates = Array[DataFrame](APILangLinks.obtainErrorID().toDF("id2"), APIPageView.obtainErrorID().toDF("id2"), APIRedirect.obtainErrorID().toDF("id2"))

      val errorDst = errorDstDuplicates.reduce(_ union _).dropDuplicates().toDF("id2")

      /*
      this.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksTranslatedDetails.txt", APILangLinks.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorViewTranslatedDetails.txt",      APIPageView.obtainErrorDetails())
      this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",  APIRedirect.obtainErrorDetails())
      */

      (resultSrc, errorSrc, resultDst, errorDst)
    }

    def DEBUG_newDataFrame(inputFolderName: Array[String], sparkSession: SparkSession): Unit = {
      //per convertire RDD in DataFrame

      var allParquetFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= inputFolderName

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        //println(folder.toString)

        val temp = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString.takeRight(2))

        if(temp.contains("it") && temp.contains("en"))
          this.DEBUG_joinDataFrame(folder.toString, sparkSession)

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders

      }

      /*while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(2) == subFolder) {

          val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

          allParquetFiles = allParquetFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val dataFrameFilesSrc = allParquetFiles map (tempFile => sparkSession.read.parquet(tempFile))

      //merge dei parquet in un dataFrame unico
      val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

      dataFrameSrc*/
    }

    def DEBUG_joinDataFrame(folder: String, sparkSession: SparkSession): Unit = {

      import sparkSession.implicits._

      val dataFrameSrc = this.dataFrameFromFoldersRecursively(Array(folder), "en", sparkSession)
      val dataFrameDst = this.dataFrameFromFoldersRecursively(Array(folder), "it", sparkSession)

      var mappa = Map[String, String]().withDefaultValue("")

      dataFrameDst.foreach(row => {

        mappa += (row.getString(0) -> row.getString(5))
      })

      val res = dataFrameSrc.map(row => {

        //val idIta = dataFrameDst.filter("id == '" + row.getString(2) +"'").first().getString(0)

        (row.getString(0), row.getInt(1), row.getString(2), row.getAs[mutable.WrappedArray[Int]](3), row.getAs[mutable.WrappedArray[Int]](4), row.getInt(5), row.getString(6), mappa(row.getString(2)))
      }).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect", "id_ita")

      res.filter("id_ita != ''").show(10, false)

    }

    def writeFileID(filePath: String, listID: Vector[String], sparkContext: SparkContext): Unit = {
      sparkContext.parallelize(listID).coalesce(1).saveAsTextFile(filePath)
    }

    def writeFileErrors(filePath: String, listErrors: Vector[(String, Vector[(Int, String)])]): Unit = {
      val file = new File(filePath)
      if(!file.exists) file.createNewFile()
      val bw = new BufferedWriter(new FileWriter(file, true))
      listErrors.foreach( ErrorTuple => bw.write(ErrorTuple._1 + ": " + ErrorTuple._2.map(tuple => "(" + tuple._1 + " , " + tuple._2  + ")" + "\t") + "\n"))
      bw.close()
    }

    def memoryInfo(): Unit = {
      val mb = 1024*1024
      val runtime = Runtime.getRuntime
      println("ALL RESULTS IN MB")
      println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      println("** Free Memory:  " + runtime.freeMemory / mb)
      println("** Total Memory: " + runtime.totalMemory / mb)
      println("** Max Memory:   " + runtime.maxMemory / mb)
    }
  }
}