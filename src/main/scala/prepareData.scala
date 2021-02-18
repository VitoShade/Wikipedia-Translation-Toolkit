import java.net.URLDecoder
import API._
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities._
import org.apache.spark.sql.functions.{col, collect_set, sum, udf, when}
import scala.collection.mutable
import scala.collection.mutable.{WrappedArray => WA}


object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val startTime = System.currentTimeMillis()

    //raccolta di tutti i file .txt nella cartella di input
    val nFile = args.drop(1).length
    val bucket = args(0)


    val errorFolderName   = bucket + "error/"
    //val folderSeparator   = "/"
    val outputFolderName  = bucket + "datiFinali/"
    //val outputErrorFolderName  = bucket + "datiFinali/error/"
    val sizeFolderName    = bucket + "datiFinali/size/"

    // Unione dei DataFrame dai parquet inglesi
    val dataFramesEn = args.slice(1, nFile/2+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
    var dataFrameSrc = dataFramesEn.reduce(_ union _)

    // Unione dei DataFrame italiani
    val dataFramesIt = args.slice(nFile/2+1, nFile+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
    var dataFrameDst = dataFramesIt.reduce(_ union _)

    // Retry errori durante downloadData e pulizia link a pagine italiane
    /*
    FileUtils.deleteDirectory(new File(errorFolderName))
    FileUtils.forceMkdir(new File(errorFolderName))
    */

    var errorPagesSrc = sparkSession.read.textFile(errorFolderName + "errors.txt").toDF("id2")
    var errorPagesDst = sparkSession.read.textFile(errorFolderName + "errorsTranslated.txt").toDF("id2")


    val (resultSrc1, errorSrc1, resultDst1, errorDst1) = DataFrameUtility.retryPagesWithErrorAndReplace(dataFrameSrc, dataFrameDst, errorPagesSrc, errorPagesDst, sparkSession)

    dataFrameSrc = resultSrc1
    errorPagesSrc = errorSrc1
    dataFrameDst = resultDst1
    errorPagesDst = errorDst1



    APILangLinks.resetErrorList()
    APIPageView.resetErrorList()
    APIRedirect.resetErrorList()

    // Compressione dataframe da tradurre (togliendo redirect)
    val compressedSrc = compressRedirect(dataFrameSrc, sparkSession)

    // Chiamata per scaricare pagine italiane che si ottengono tramite redirect
    dataFrameDst = missingIDsDF(dataFrameDst, sparkSession).dropDuplicates()

    val errorMissingIDDuplicates = Array[DataFrame](APILangLinks.obtainErrorID().toDF("id2"), APIPageView.obtainErrorID().toDF("id2"), APIRedirect.obtainErrorID().toDF("id2"))

    val errorMissingID = errorMissingIDDuplicates.reduce(_ union _).dropDuplicates().toDF("id2")

    // Cancellazione pagine con errori
    val (resultDataFrameSrc, resultDataFrameDst) = removeErrorPages(compressedSrc, dataFrameDst, errorMissingID, errorPagesSrc, errorPagesDst)

    // Creazione DataFrame dimensioni
    val dimPageDF = makeDimDF(resultDataFrameSrc, resultDataFrameDst, sparkSession)

    resultDataFrameSrc.coalesce(1).write.parquet(outputFolderName + "en")
    resultDataFrameDst.coalesce(1).write.parquet(outputFolderName + "it")
    dimPageDF.coalesce(1).write.parquet(sizeFolderName)

    //Controllo se è corretto
    //val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)
    //dimPageDF.filter(removeEmpty($"id_traduzioni_redirect")).show(10, false)

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

  def compressRedirect(dataFrameSrc: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val explodedSrc = dataFrameSrc.select("id", "num_traduzioni", "id_redirect", "id_pagina_tradotta","num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")
      .map(row => ( row.getAs[String](0),
        row.getAs[Int](1),
        row.getAs[String](2),
        row.getAs[String](3),
        row.getAs[WA[Int]](4)(0),
        row.getAs[WA[Int]](4)(1),
        row.getAs[WA[Int]](4)(2),
        row.getAs[WA[Int]](5)(0),
        row.getAs[WA[Int]](5)(1),
        row.getAs[WA[Int]](5)(2),
        row.getAs[WA[Int]](5)(3),
        row.getAs[WA[Int]](5)(4),
        row.getAs[WA[Int]](5)(5),
        row.getAs[WA[Int]](5)(6),
        row.getAs[WA[Int]](5)(7),
        row.getAs[WA[Int]](5)(8),
        row.getAs[WA[Int]](5)(9),
        row.getAs[WA[Int]](5)(10),
        row.getAs[WA[Int]](5)(11),
        row.getAs[Int](6)
      )
      ).toDF("id", "num_traduzioni", "id_redirect", "id_pagina_tradotta", "num_visualiz_anno1", "num_visualiz_anno2", "num_visualiz_anno3", "num_visualiz_mesi1", "num_visualiz_mesi2","num_visualiz_mesi3","num_visualiz_mesi4","num_visualiz_mesi5","num_visualiz_mesi6","num_visualiz_mesi7","num_visualiz_mesi8","num_visualiz_mesi9","num_visualiz_mesi10","num_visualiz_mesi11","num_visualiz_mesi12","byte_dim_page")

    //somma del numero di visualizzazioni per pagine che sono redirect
    val redirectSrc = explodedSrc.filter("id_redirect != ''")
      .drop("num_traduzioni")
      .drop("byte_dim_page")
      .groupBy("id_redirect")
      .agg(
        sum($"num_visualiz_anno1"),
        sum($"num_visualiz_anno2"),
        sum($"num_visualiz_anno3"),
        sum($"num_visualiz_mesi1"),
        sum($"num_visualiz_mesi2"),
        sum($"num_visualiz_mesi3"),
        sum($"num_visualiz_mesi4"),
        sum($"num_visualiz_mesi5"),
        sum($"num_visualiz_mesi6"),
        sum($"num_visualiz_mesi7"),
        sum($"num_visualiz_mesi8"),
        sum($"num_visualiz_mesi9"),
        sum($"num_visualiz_mesi10"),
        sum($"num_visualiz_mesi11"),
        sum($"num_visualiz_mesi12"),
        collect_set(when(!(col("id_pagina_tradotta") === ""), col("id_pagina_tradotta"))).as("id_traduzioni_redirect")
      ).withColumnRenamed("id_redirect","id")


    //somma del numero di visualizzazioni delle redirect alle pagine principali
    explodedSrc.filter("id_redirect == ''")
      .join(redirectSrc, Seq("id"), "left_outer")
      .map(row => ( row.getAs[String](0),
        row.getAs[Int](1),
        row.getAs[String](3),
        (4 to 6) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
        (7 to 18) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
        row.getAs[Int](19),
        if(row.getAs[WA[String]](35) != null) row.getAs[WA[String]](35) else WA.empty[String]
      )
      ).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

  }

  def missingIDsDF(dataFrameDst: DataFrame, sparkSession: SparkSession) = {

    //val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._

    val idDF = dataFrameDst.select("id").rdd.map(_.getAs[String](0)).collect().toList

    val dataFrame = dataFrameDst.union(dataFrameDst.filter(!(col("id_redirect") === "") && !col("id_redirect").isin(idDF: _*)).select("id_redirect").map(line => {
      val tuple1 = APILangLinks.callAPI(line.getAs[String](0), "it", "en")
      val tuple2 = APIPageView.callAPI(line.getAs[String](0), "it")
      val tuple3 = APIRedirect.callAPI(line.getAs[String](0), "it")

      (line.getAs[String](0), URLDecoder.decode(tuple1._2,  "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)
    }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")).persist

    //salvataggio degli errori per le API di it.wikipedia
    //DataFrameUtility.writeFileID(errorFolderName + "errorLangLinksMissingIDsDF", APILangLinks.obtainErrorID(), sparkContext)
    //DataFrameUtility.writeFileID(errorFolderName + "errorViewMissingIDsDF",      APIPageView.obtainErrorID(), sparkContext)
    //DataFrameUtility.writeFileID(errorFolderName + "errorRedirectMissingIDsDF",  APIRedirect.obtainErrorID(), sparkContext)

    dataFrame
  }

  def removeErrorPages(compressedSrc: DataFrame, dataFrameDst: DataFrame, errorMissingID: DataFrame, errorPagesSrc: DataFrame, errorPagesDst: DataFrame) = {

    //val errorSrcFiles = Array("errorLangLinks.txt", "errorView.txt", "errorRedirect.txt")
    //val errorDstFiles = Array("errorLangLinksTranslated.txt", "errorViewTranslated.txt", "errorRedirectTranslated.txt")

    //val errorSrcFile = "errors.txt"
    //val errorDstFile = "errorsTranslated.txt"

    //pagine inglesi che hanno avuto errori con le API
    //val errorPagesSrc = sparkSession.read.textFile(errorFolderName + errorSrcFile).toDF("id2")
    //val errorPagesSrc = DataFrameUtility.collectErrorPagesFromFoldersRecursively(errorSrcFiles.map (x => bucket + x), sparkSession).toDF("id2")

    //sottoinsieme delle pagine inglesi compresse che hanno avuto problemi
    val joinedCompressedSrc = compressedSrc.join(errorPagesSrc, compressedSrc("id") === errorPagesSrc("id2"), "inner").
      select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //rimozione dalle pagine compresse di quelle con errori
    val resultDataFrameSrc = compressedSrc.except(joinedCompressedSrc).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //pagine italiane che hanno avuto errori con le API
    //var errorPagesDst = sparkSession.read.textFile(errorFolderName + errorDstFile).toDF("id2")
    var errorPagesDstLocal = errorPagesDst
    //val errorPagesDst = DataFrameUtility.collectErrorPagesFromFoldersRecursively(errorDstFiles.map (x => bucket + x), sparkSession).toDF("id2")

    errorPagesDstLocal = errorPagesDstLocal.union(errorMissingID).dropDuplicates()

    //sottoinsieme delle pagine italiane compresse che hanno avuto problemi
    val joinedCompressedDst = dataFrameDst.join(errorPagesDstLocal, dataFrameDst("id") === errorPagesDstLocal("id2"), "inner").
      select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    //rimozione dalle pagine compresse di quelle con errori
    val resultDataFrameDst = dataFrameDst.except(joinedCompressedDst).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    (resultDataFrameSrc, resultDataFrameDst)
  }

  def makeDimDF(mainDF: DataFrame, transDF: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    //crea una Map[String, Tuple(String, Int)] con: chiave = id della pagina italiana; valore = (id_redirect, dim)
    //se la pagina non italiana non ha redirect, allora dentro id_redirect salva l'id della pagina.
    //La dimensione è quella della pagina italiana e non della redirect.
    val mappa = transDF.map(row =>
      (row.getString(0), Tuple2(if(row.getString(5).nonEmpty) row.getString(5) else row.getString(0), row.getInt(4)))
    ).collect().toMap.withDefaultValue(Tuple2("",0))

    //Per ogni pagina italiana che è redirect, cambia il valore dim con la dim della pagina puntata
    val mappa2 = mappa.keys.map(id => {
      val (originalID, dim) = mappa(id)
      if(originalID==id) {(id,Tuple2(originalID, dim))} else {(id,Tuple2(originalID, mappa(originalID)._2))}
    }).toMap.withDefaultValue(Tuple2("",0))

    val res = mainDF.map(row => {
      val (redirectID, dimRedirect) = mappa2(row.getString(2))
      val arr = row.getAs[mutable.WrappedArray[String]](6).map(redirectID => mappa2(redirectID)._1)
      (row.getString(0), row.getInt(5), arr, redirectID, dimRedirect, arr.map(redirectID => mappa2(redirectID)._2).sum)
    }).toDF("id",  "byte_dim_page", "id_traduzioni_redirect", "id_ita", "byte_dim_page_ita_original", "id_traduzioni_redirect_dim")

    val sumDF = res.groupBy("id_ita")
      .sum("byte_dim_page")
      .withColumnRenamed("sum(byte_dim_page)","byte_dim_page_tot")
      .withColumnRenamed("id_ita", "id_ita2")
      //.repartition(DataFrameUtility.numPartitions)

    res.join(sumDF, res("id_ita") === sumDF("id_ita2")).drop("id_ita2")
  }
}