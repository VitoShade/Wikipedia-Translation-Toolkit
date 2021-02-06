import java.io.File
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import API.{APILangLinks, APIPageView, APIRedirect}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{col, collect_set, sum, udf, when}

import scala.collection.mutable
import scala.collection.mutable.{WrappedArray => WA}


object prepareData extends App {
  def mainX(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[16]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName  = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/outputProcessati"
    val tempFolderName   = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/tempOutput"

    val outputFolderName = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/datiFinali"
    val errorFolderName  = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/tempOutput/error"
    val sizeFolderName   = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/datiFinali/size"
    val folderSeparator  = "/"

    val startTime = System.currentTimeMillis()

    // Retry errori durante downloadData e pulizia link a pagine italiane
    /*FileUtils.deleteDirectory(new File(errorFolderName))
    FileUtils.forceMkdir(new File(errorFolderName))
    DataFrameUtility.retryPagesWithErrorAndReplace(inputFolderName, tempFolderName, errorFolderName, folderSeparator, sparkSession)*/

    APILangLinks.resetErrorList()
    APIPageView.resetErrorList()
    APIRedirect.resetErrorList()

    // DataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(tempFolderName), "en", sparkSession)

    // Compressione dataframe da tradurre (togliendo redirect)
    val compressedSrc = compressRedirect(dataFrameSrc, sparkSession).repartition(DataFrameUtility.numPartitions)

    // DataFrame dai parquet italiani
    var dataFrameDst = DataFrameUtility.dataFrameFromFoldersRecursively(Array(tempFolderName), "it", sparkSession).dropDuplicates().repartition(DataFrameUtility.numPartitions)

    // Chiamata per scaricare pagine italiane che si ottengono tramite redirect
    dataFrameDst = missingIDsDF(dataFrameDst, errorFolderName, folderSeparator, sparkSession).dropDuplicates().repartition(DataFrameUtility.numPartitions)

    // Cancellazione pagine con errori
    val (resultDataFrameSrc, resultDataFrameDst) = removeErrorPages(compressedSrc, dataFrameDst, sparkSession, tempFolderName)

    // Creazione DataFrame dimensioni
    val dimPageDF = makeDimDF(resultDataFrameSrc, resultDataFrameDst, sparkSession)

    // Pulizia directory
    FileUtils.deleteDirectory(new File(outputFolderName + folderSeparator + "en"))
    FileUtils.deleteDirectory(new File(outputFolderName + folderSeparator + "it"))
    FileUtils.deleteDirectory(new File(sizeFolderName))

    resultDataFrameSrc.write.parquet(outputFolderName + folderSeparator + "en")
    resultDataFrameDst.write.parquet(outputFolderName + folderSeparator + "it")
    dimPageDF.repartition(DataFrameUtility.numPartitions).write.parquet(sizeFolderName)

    //Controllo se è corretto
    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)
    dimPageDF.filter(removeEmpty($"id_traduzioni_redirect")).show(10, false)

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

  def missingIDsDF(dataFrameDst: DataFrame, errorFolderName: String, folderSeparator: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val idDF = dataFrameDst.select("id").rdd.map(_.getAs[String](0)).collect().toList

    val dataFrame = dataFrameDst.union(dataFrameDst.filter(!(col("id_redirect") === "") && !(col("id_redirect").isin(idDF: _*))).select("id_redirect").map(line => {
      val tuple1 = APILangLinks.callAPI(line.getAs[String](0), "it", "en")
      val tuple2 = APIPageView.callAPI(line.getAs[String](0), "it")
      val tuple3 = APIRedirect.callAPI(line.getAs[String](0), "it")

      (line.getAs[String](0), URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)
    }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")).persist

    //salvataggio degli errori per le API di it.wikipedia
    DataFrameUtility.writeFileID(errorFolderName + folderSeparator + "errorLangLinksTranslated.txt", APILangLinks.obtainErrorID())
    DataFrameUtility.writeFileID(errorFolderName + folderSeparator + "errorViewTranslated.txt",      APIPageView.obtainErrorID())
    DataFrameUtility.writeFileID(errorFolderName + folderSeparator + "errorRedirectTranslated.txt",  APIRedirect.obtainErrorID())

    DataFrameUtility.writeFileErrors(errorFolderName + folderSeparator + "errorLangLinksTranslatedDetails.txt", APILangLinks.obtainErrorDetails())
    DataFrameUtility.writeFileErrors(errorFolderName + folderSeparator + "errorViewTranslatedDetails.txt",      APIPageView.obtainErrorDetails())
    DataFrameUtility.writeFileErrors(errorFolderName + folderSeparator + "errorRedirectTranslatedDetails.txt",  APIRedirect.obtainErrorDetails())

    dataFrame
  }

  def removeErrorPages(compressedSrc: DataFrame, dataFrameDst:DataFrame, sparkSession: SparkSession, tempFolderName: String ) = {
    //pagine inglesi che hanno avuto errori con le API
    val errorPagesSrc = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(tempFolderName), sparkSession, false).toDF("id2")

    //sottoinsieme delle pagine inglesi compresse che hanno avuto problemi
    val joinedCompressedSrc = compressedSrc.join(errorPagesSrc, compressedSrc("id") === errorPagesSrc("id2"), "inner").
      select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //rimozione dalle pagine compresse di quelle con errori
    val resultDataFrameSrc = compressedSrc.except(joinedCompressedSrc)

    //pagine italiane che hanno avuto errori con le API
    val errorPagesDst = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(tempFolderName), sparkSession, true).toDF("id2")

    //sottoinsieme delle pagine italiane compresse che hanno avuto problemi
    val joinedCompressedDst = dataFrameDst.join(errorPagesDst, dataFrameDst("id") === errorPagesDst("id2"), "inner").
      select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    //rimozione dalle pagine compresse di quelle con errori
    val resultDataFrameDst = dataFrameDst.except(joinedCompressedDst)

    (resultDataFrameSrc.repartition(DataFrameUtility.numPartitions), resultDataFrameDst.repartition(DataFrameUtility.numPartitions))
  }

  def makeDimDF(mainDF: DataFrame, transDF: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    //crea una Map[String, Tuple(String, Int)] con: chiave = id della pagina italiana; valore = (id_redirect, dim)
    //se la pagina non italiana non ha redirect, allora dentro id_redirect salva l'id della pagina.
    //La dimensione è quella della pagina italiana e non della redirect.
    val mappa = transDF.map(row =>
      (row.getString(0), Tuple2(if(!row.getString(5).isEmpty) row.getString(5) else row.getString(0), row.getInt(4)))
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
      .repartition(DataFrameUtility.numPartitions)

    res.join(sumDF, res("id_ita") === sumDF("id_ita2")).drop("id_ita2")
  }
}