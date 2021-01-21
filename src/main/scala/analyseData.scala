import org.apache.spark.sql.SparkSession
import Utilities._
import org.apache.spark.sql.functions.desc
import scala.collection.mutable.{WrappedArray => WA }

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    import sparkSession.implicits._

    val inputFolderName = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/result"

    //val tempFolderName    = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/tempResult"
    //val outputFolderName  = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/result"
    //val errorFolderName   = "/Users/stefano/IdeaProjects/Wikipedia-Translation-Toolkit/src/main/files/error"
    val folderSeparator   = "/"

    val inputFolders = Array(inputFolderName + folderSeparator + "en")

    val allInputFolders = DataFrameUtility.collectParquetFilesFromFolders(inputFolders)

    val dataFrameFilesSrc = allInputFolders map (tempFile => sparkSession.read.parquet(tempFile))

    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    val query = dataFrameSrc.select("id", "num_traduzioni", "id_redirect", "id_pagina_tradotta","num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")
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

    val redirect = query.filter("id_redirect!=''")
                        .drop("num_traduzioni")
                        .drop("byte_dim_page")
                        .groupBy("id_redirect")
                        .sum()
                        .withColumnRenamed("id_redirect","id")

    val resQuery = query.filter("id_redirect==''")
                        .join(redirect, Seq("id"), "left_outer")
                        .na.fill(0)
                        .map(row => ( row.getAs[String](0),
                                      row.getAs[Int](1),
                                      row.getAs[String](3),
                                      (4 to 6) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                                      (7 to 18) map ( i => row.getAs[Int](i)+row.getAs[Long](i+16)),
                                      row.getAs[Int](19)
                                    )
                        ).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    resQuery.orderBy(desc("byte_dim_page")).show(false)
    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
    }

}
