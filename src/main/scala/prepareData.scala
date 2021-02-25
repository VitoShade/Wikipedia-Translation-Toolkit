import java.net.URLDecoder
import API._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, explode, sum, when}

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

    val nFile = args.drop(1).length
    val bucket = args(0)

    val errorFolderName   = bucket + "error/"
    val outputFolderName  = bucket + "datiFinali/"
    val sizeFolderName    = bucket + "datiFinali/size/"

    /*

    // Unione dei DataFrame dai parquet inglesi
    val dataFramesEn = args.slice(1, nFile/2+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
    var dataFrameSrc = dataFramesEn.reduce(_ union _)

    // Unione dei DataFrame italiani
    val dataFramesIt = args.slice(nFile/2+1, nFile+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
    var dataFrameDst = dataFramesIt.reduce(_ union _)


     */

    val dataFrameSrc = sparkSession.read.parquet(bucket + args(1)).repartition(40)
    var dataFrameDst = sparkSession.read.parquet(bucket + args(2)).repartition(4)

    val errorPagesSrc = sparkSession.read.textFile(errorFolderName + "errors.txt").toDF("id2")
    val errorPagesDst = sparkSession.read.textFile(errorFolderName + "errorsTranslated.txt").toDF("id2")

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

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

  def compressRedirect(dataFrameSrc: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val explodedSrc = dataFrameSrc.select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")
      .map(row => ( row.getAs[String](0),
        row.getAs[Int](1),
        row.getAs[String](6),
        row.getAs[String](2),
        row.getAs[WA[Int]](3)(0),
        row.getAs[WA[Int]](3)(1),
        row.getAs[WA[Int]](3)(2),
        row.getAs[WA[Int]](4)(0),
        row.getAs[WA[Int]](4)(1),
        row.getAs[WA[Int]](4)(2),
        row.getAs[WA[Int]](4)(3),
        row.getAs[WA[Int]](4)(4),
        row.getAs[WA[Int]](4)(5),
        row.getAs[WA[Int]](4)(6),
        row.getAs[WA[Int]](4)(7),
        row.getAs[WA[Int]](4)(8),
        row.getAs[WA[Int]](4)(9),
        row.getAs[WA[Int]](4)(10),
        row.getAs[WA[Int]](4)(11),
        row.getAs[Int](5)
      )
      ).toDF("id", "num_traduzioni", "id_redirect", "id_pagina_tradotta", "num_visualiz_anno1", "num_visualiz_anno2", "num_visualiz_anno3", "num_visualiz_mesi1", "num_visualiz_mesi2","num_visualiz_mesi3","num_visualiz_mesi4","num_visualiz_mesi5","num_visualiz_mesi6","num_visualiz_mesi7","num_visualiz_mesi8","num_visualiz_mesi9","num_visualiz_mesi10","num_visualiz_mesi11","num_visualiz_mesi12","byte_dim_page")

    //somma del numero di visualizzazioni per pagine che sono redirect
    val redirectSrc = explodedSrc.filter("id_redirect != ''")
      .drop("num_traduzioni", "byte_dim_page")
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
      .map(row => (
        row.getAs[String](0),
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
    import sparkSession.implicits._

    val dataFrame = dataFrameDst.union(dataFrameDst.filter(!(col("id_redirect") === "")).select("id_redirect").map(line => {
      val tuple1 = APILangLinks.callAPI(line.getAs[String](0), "it", "en")
      val tuple2 = APIPageView.callAPI(line.getAs[String](0), "it")
      val tuple3 = APIRedirect.callAPI(line.getAs[String](0), "it")

      (line.getAs[String](0), URLDecoder.decode(tuple1._2,  "UTF-8"), tuple2._1, tuple2._2, tuple3._1, tuple3._2)
    }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect"))

    dataFrame
  }

  def removeErrorPages(compressedSrc: DataFrame, dataFrameDst: DataFrame, errorMissingID: DataFrame, errorPagesSrc: DataFrame, errorPagesDst: DataFrame) = {
    //sottoinsieme delle pagine inglesi compresse che hanno avuto problemi
    val joinedCompressedSrc = compressedSrc.join(errorPagesSrc, compressedSrc("id") === errorPagesSrc("id2"), "left_outer")

    val resultDataFrameSrc = joinedCompressedSrc.filter(joinedCompressedSrc.col("id2").isNull).select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //pagine italiane che hanno avuto errori con le API
    val errorPagesDstLocal = errorPagesDst.union(errorMissingID).dropDuplicates().toDF("id2")

    //sottoinsieme delle pagine italiane compresse che hanno avuto problemi
    val joinedCompressedDst = dataFrameDst.join(errorPagesDstLocal, dataFrameDst("id") === errorPagesDstLocal("id2"), "left_outer")

    val resultDataFrameDst = joinedCompressedDst.filter(joinedCompressedDst.col("id2").isNull).select("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    (resultDataFrameSrc, resultDataFrameDst)
  }

  def makeDimDF(mainDF: DataFrame, transDF: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val df1 = transDF.map(row => {
      (row.getString(0), if(row.getString(5).nonEmpty) row.getString(5) else row.getString(0), row.getInt(4))
    }).toDF("id2", "id_redirect2", "dim2")

    val df3 = df1.join(transDF, df1("id_redirect2")===transDF("id"), "left_outer")
      .na.fill("", Seq("id_pagina_originale"))
      .na.fill(0, Seq("byte_dim_page"))
      .map(row => {
      val id = row.getString(0)
      val id_redirect = row.getString(1)
      val dim = if(row.getString(1) == row.getString(0)) row.getInt(2) else row.getInt(7)
      (id, id_redirect, dim)
      }
    ).toDF("id2", "id_redirect2", "dim2").dropDuplicates()

    val res = mainDF.join(df3, mainDF("id_pagina_tradotta")===df3("id2"), "left_outer")
      .na.fill("", Seq("id2"))
      .na.fill("", Seq("id_redirect2"))
      .na.fill(0, Seq("dim2"))
      .map( row => {
        val id = row.getString(0)
        val byte_dim_page = row.getInt(5)
        val id_traduzioni_redirect = row.getAs[mutable.WrappedArray[String]](6)
        val id_ita = row.getString(8)
        val byte_dim_page_ita_original = row.getInt(9)
        val id_traduzioni_redirect_dim = 0
        (id, byte_dim_page, id_traduzioni_redirect, id_ita, byte_dim_page_ita_original, id_traduzioni_redirect_dim)
      }
    ).toDF("id",  "byte_dim_page", "id_traduzioni_redirect", "id_ita", "byte_dim_page_ita_original", "id_traduzioni_redirect_dim")

    val redirect = res.select($"id", explode($"id_traduzioni_redirect")).toDF("id_original", "id_redirect_exploded")

    val DF_join = df3.join(redirect, df3("id2")===redirect("id_redirect_exploded")).map(row => {
      val id = row.getString(3)
      val redirect_dim = row.getInt(2)
      (id, redirect_dim)
    }).toDF("id3","dim_redirect3").groupBy("id3")
      .sum("dim_redirect3")
      .withColumnRenamed("sum(dim_redirect3)","id_traduzioni_redirect_dim3")

    val finalRes = res.join(DF_join, res("id")===DF_join("id3"), "left_outer").na.fill(0, Seq("id_traduzioni_redirect_dim3")).map(row => {
      val id = row.getString(0)
      val byte_dim_page = row.getInt(1)
      val id_ita = row.getString(3)
      val byte_dim_page_ita_original = row.getInt(4)
      val id_traduzioni_redirect_dim = row.getLong(7)
      (id, byte_dim_page, id_ita, byte_dim_page_ita_original, id_traduzioni_redirect_dim)
    }).toDF("id", "byte_dim_page", "id_ita", "byte_dim_page_ita_original", "id_traduzioni_redirect_dim")

    val sumDF = finalRes.groupBy("id_ita")
      .sum("byte_dim_page")
      .withColumnRenamed("sum(byte_dim_page)","byte_dim_page_tot")
      .withColumnRenamed("id_ita", "id_ita2")

    res.join(sumDF, res("id_ita") === sumDF("id_ita2")).drop("id_ita2")
  }
}