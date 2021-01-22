import org.apache.spark.sql.SparkSession
import Utilities._
import org.apache.spark.sql.functions.{asc, col, collect_list, desc, sum, when}

import scala.collection.mutable.{WrappedArray => WA}

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[16]").appName("analyseData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val startTime = System.currentTimeMillis()

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati\\File1-3"
    //val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    /*val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result\\error"*/
    val folderSeparator = "\\"

    //cartella parquet inglesi
    val allInputFoldersSrc = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "en")

    val dataFrameFilesSrc = allInputFoldersSrc map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    //cartella parquet italiani
    val allInputFoldersDst = DataFrameUtility.collectParquetFromFoldersRecursively(Array(inputFolderName), "it")

    val dataFrameFilesDst = allInputFoldersDst map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un dataFrame unico
    val dataFrameDst = dataFrameFilesDst.reduce(_ union _)


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
        collect_list(when(!(col("id_pagina_tradotta") === ""), col("id_pagina_tradotta"))).as("id_traduzioni_redirect")
      ).withColumnRenamed("id_redirect","id")


    //somma del numero di visualizzazioni delle redirect alle pagine principali
    val compressedSrc = explodedSrc.filter("id_redirect == ''")
                        .join(redirectSrc, Seq("id"), "left_outer")
                        .na.fill(0)
                        .map(row => ( row.getAs[String](0),
                                      row.getAs[Int](1),
                                      row.getAs[String](3),
                                      (4 to 6) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                                      (7 to 18) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                                      row.getAs[Int](19),
                                      row.getAs[WA[String]](35)
                                    )
                        ).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect")

    //compressedSrc.show(false)

    compressedSrc.orderBy(desc("id_traduzioni_redirect")).show(false)

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

}
