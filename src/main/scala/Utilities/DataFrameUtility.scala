import java.io._
import scala.collection.mutable
import org.apache.spark.sql._
import scalaj.http.Http
import API.{APILangLinks, APIPageView, APIRedirect}
import java.net._
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
      //per convertire RDD in DataFrame
      import sparkSession.implicits._

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      val tempResultSrc = errorPagesSrc.map(page => {
        val line = page.getString(0)
        //chiamata alle API per en.wikipedia e creazione di un record del DataFrame
        val tuple1 = APILangLinks.callAPI(line, "en", "it")
        val tuple2 = APIPageView.callAPI(line, "en")
        val tuple3 = APIRedirect.callAPI(line, "en")

        (line, tuple1._1, URLDecoder.decode(tuple1._2, "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)
      })

      //creazione del DataFrame per en.wikipedia
      val tempDataFrameSrc = tempResultSrc.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      tempDataFrameSrc.persist

      val joinedDataFrameSrc = dataFrameSrc.join(errorPagesSrc, dataFrameSrc("id") === errorPagesSrc("id2"), "inner").
        select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameSrc = dataFrameSrc.except(joinedDataFrameSrc)//.repartition(numPartitions)

      val resultSrc = noErrorDataFrameSrc.union(tempDataFrameSrc).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val errorSrcDuplicates = Array[DataFrame](APILangLinks.obtainErrorID().toDF("id2"), APIPageView.obtainErrorID().toDF("id2"), APIRedirect.obtainErrorID().toDF("id2"))

      val errorSrc = errorSrcDuplicates.reduce(_ union _).dropDuplicates().toDF("id2")

      //reset degli errori
      APILangLinks.resetErrorList()
      APIPageView.resetErrorList()
      APIRedirect.resetErrorList()

      //pagine italiane da scaricare perché trovate con il retry della pagina inglese
      val newDstPages = tempDataFrameSrc.select("id_pagina_tradotta").filter("id_pagina_tradotta != ''").toDF("id2")

      val errorPagesDst2 = errorPagesDst.union(newDstPages)

      val dstPagesWithHash = errorPagesDst2.filter(x => {x.getString(0).contains("#")})

      val dstPagesToRetryWithoutHash = errorPagesDst2.except(dstPagesWithHash)

      val dstPagesToRetryWithHash = dstPagesWithHash.map(x => {
        x.getString(0).split("#")(0)
      }).toDF("id2")

      val dstPagesToRetry = dstPagesToRetryWithoutHash.union(dstPagesToRetryWithHash)

      val tempResultDst = dstPagesToRetry.map(page => {
        val line = page.getString(0)
        //chiamata alle API per it.wikipedia e creazione di un record del DataFrame
        val tuple1 = APILangLinks.callAPI(line, "it", "en")
        val tuple2 = APIPageView.callAPI(line, "it")
        val tuple3 = APIRedirect.callAPI(line, "it")

        (line, URLDecoder.decode(tuple1._2, "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)

      })

      val tempDataFrameDst = tempResultDst.toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val joinedDataFrameDst = dataFrameDst.join(errorPagesDst2, dataFrameDst("id") === errorPagesDst2("id2"), "inner").
        select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //rimozione dalle pagine compresse di quelle con errori
      val noErrorDataFrameDst = dataFrameDst.except(joinedDataFrameDst)//.repartition(numPartitions)

      val resultDst = noErrorDataFrameDst.union(tempDataFrameDst).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      val errorDstDuplicates = Array[DataFrame](APILangLinks.obtainErrorID().toDF("id2"), APIPageView.obtainErrorID().toDF("id2"), APIRedirect.obtainErrorID().toDF("id2"))

      val errorDst = errorDstDuplicates.reduce(_ union _).dropDuplicates().toDF("id2")

      (resultSrc, errorSrc, resultDst, errorDst)
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

  }
}