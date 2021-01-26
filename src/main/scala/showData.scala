import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import Utilities._
import org.apache.commons.io.FileUtils
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.desc




object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[30]").appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")


    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\error"
    val folderSeparator = "\\"

    /*FileUtils.deleteDirectory(new File(outputFolderName))
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
    this.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",  APIRedirect.obtainErrorDetails())*/



    /*val res = DataFrameUtility.DEBUG_collectSelectedErrorsFromFoldersRecursively(Array(inputFolderName), sparkSession, true, "errorView")

    val res2 = res.rdd.map(x => {if (x.contains("#")) x else ""})

    res2.foreach(x => {if(x != "") println(x)})*/


    /*val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "it")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)


    println(dataFrameSrc.filter("id_redirect != ''").count())

    dataFrameSrc.filter("id_redirect != ''").show(50, false)*/



    sparkSession.stop()
  }
}