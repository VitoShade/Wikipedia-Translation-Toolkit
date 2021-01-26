import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import API.{APILangLinks, APIPageView, APIRedirect}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities._
import org.apache.spark.sql.functions.{col, collect_set, desc, sum, when}

import scala.collection.mutable.{WrappedArray => WA}


object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[16]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    import sparkSession.implicits._

    val inputFolderName = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/fileProcessati/File1-10"
    val folderSeparator = "/"

    //cartella parquet inglesi
    val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "en")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

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

    //val rowProva = Seq(("prova123", 0, "Navapur", "tradotta123", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)).toDF("id", "num_traduzioni", "id_redirect", "id_pagina_tradotta", "num_visualiz_anno1", "num_visualiz_anno2", "num_visualiz_anno3", "num_visualiz_mesi1", "num_visualiz_mesi2","num_visualiz_mesi3","num_visualiz_mesi4","num_visualiz_mesi5","num_visualiz_mesi6","num_visualiz_mesi7","num_visualiz_mesi8","num_visualiz_mesi9","num_visualiz_mesi10","num_visualiz_mesi11","num_visualiz_mesi12","byte_dim_page")

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
    val compressedSrc = explodedSrc.filter("id_redirect == ''")
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

    //compressedSrc.show(false)

    //compressedSrc.orderBy(desc("id_traduzioni_redirect")).show(false)

    //cartella parquet italiani
    val allInputFoldersDst = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "it")
    val dataFrameFilesDst = allInputFoldersDst map (tempFile => sparkSession.read.parquet(tempFile))
    //merge dei parquet in un dataFrame unico
    //TODO: da controllare la drop
    var dataFrameDst = dataFrameFilesDst.reduce(_ union _).dropDuplicates()

    //dataFrameDst.show(50, false)

    //val rowProva = Seq(("Astronavi_e_veicoli_di_Guerre_stellari", "tradotta123", Array(0, 0, 0), Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 0, "")).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")
    //dataFrameDst=dataFrameDst.union(rowProva)

    dataFrameDst = missingIDsDF(dataFrameDst, sparkSession).dropDuplicates()

    //dataFrameDst.show(50, false)

    val explodedDst = dataFrameDst.select("id", "id_redirect", "id_pagina_originale","num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")
                                  .map(row => ( row.getAs[String](0),
                                                row.getAs[String](1),
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
                                  ).toDF("id", "id_redirect", "id_pagina_originale", "num_visualiz_anno1", "num_visualiz_anno2", "num_visualiz_anno3", "num_visualiz_mesi1", "num_visualiz_mesi2","num_visualiz_mesi3","num_visualiz_mesi4","num_visualiz_mesi5","num_visualiz_mesi6","num_visualiz_mesi7","num_visualiz_mesi8","num_visualiz_mesi9","num_visualiz_mesi10","num_visualiz_mesi11","num_visualiz_mesi12","byte_dim_page")

    val redirectDst = explodedDst.filter("id_redirect != ''")
                                 .drop("byte_dim_page")
                                 .groupBy("id_redirect")
                                 .agg(sum($"num_visualiz_anno1"),
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
                                      collect_set(when(!(col("id_pagina_originale") === ""), col("id_pagina_originale"))).as("id_originale_redirect")
                                ).withColumnRenamed("id_redirect","id")

    val compressedDst = explodedDst.filter("id_redirect == ''")
      .join(redirectDst, Seq("id"), "left_outer")
      .map(row => ( row.getAs[String](0),
                    row.getAs[String](2),
                    (3 to 5) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                    (6 to 17) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                    row.getAs[Int](18),
                    if(row.getAs[WA[String]](34) != null) row.getAs[WA[String]](34) else WA.empty[String]
      )
      ).toDF("id", "id_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_originali_redirect")




    //pagine inglesi che hanno avuto errori con le API
    val errorPagesSrc = DataFrameUtility.collectErrorPagesFromFoldersRecursively(Array(inputFolderName), sparkSession, false).toDF("id2")

    //sottoinsieme delle pagine inglesi compresse che hanno avuto problemi
    val joinedCompressedSrc = compressedSrc.join(errorPagesSrc, compressedSrc("id") === errorPagesSrc("id2"), "inner").
      select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //rimozione dalle pagine compresse di quelle con errori
    val resultDataFrameSrc = compressedSrc.except(joinedCompressedSrc)

    val dimPageDF=makeNewDF(resultDataFrameSrc, dataFrameDst)

    //redirectDst.show(10,false)
    //compressedDst.show(20, false)

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

  def missingIDsDF(dataFrameDst: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val idDF = dataFrameDst.select("id").rdd.map(_.getAs[String](0)).collect().toList
    dataFrameDst.union(dataFrameDst.filter(!(col("id_redirect") === "") && !(col("id_redirect").isin(idDF: _*))).select("id_redirect").map(line => {
      val tuple1 = APILangLinks.callAPI(line.getAs[String](0), "it", "en")
      val tuple2 = APIPageView.callAPI(line.getAs[String](0), "it")
      val tuple3 = APIRedirect.callAPI(line.getAs[String](0), "it")
      EntryDst(line.getAs[String](0), URLDecoder.decode(tuple1._2,  StandardCharsets.UTF_8), tuple2._1, tuple2._2, tuple3._1, tuple3._2)
    }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect"))
  }

  def makeNewDF(mainDF: DataFrame, transDF: DataFrame) = {
    val tmp_DF = transDF.filter("id_redirect!=''")
                        .select("id", "id_redirect")
                        .withColumnRenamed("id","id_traduzione")
                        .withColumnRenamed("id_redirect","id_redirect_traduzione")

    val new_DF = mainDF.select("id", "id_pagina_tradotta", "byte_dim_page")
                       .join(tmp_DF, mainDF("id_pagina_tradotta")===tmp_DF("id_traduzione"))
                       .drop("id_pagina_tradotta")
                       .drop("id_traduzione")
                       .withColumnRenamed("id_redirect_traduzione","id_pagina_tradotta")
                       .withColumnRenamed("byte_dim_page","byte_dim_page_tradotta")

    //new_DF.show(50, false)

    val sum_DF = new_DF.groupBy("id_pagina_tradotta").sum().withColumnRenamed("sum(byte_dim_page)","byte_dim_page_totale")

    //sum_DF.show(50, false)

    val dims_DF = sum_DF.join(transDF.select("id", "byte_dim_page"), sum_DF("id_pagina_tradotta")===transDF("id"))
      .drop("id")
      .withColumnRenamed("id_pagina_tradotta", "id_tmp")

    new_DF.join(dims_DF, new_DF("id_pagina_tradotta")===dims_DF("id_tmp")).drop("id_tmp")
  }


  //XIV_secolo
}
