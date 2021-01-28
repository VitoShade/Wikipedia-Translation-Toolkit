import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.{WrappedArray => WA}
import org.apache.spark.sql.functions.udf

object trueAnalyse extends App {

  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[32]").appName("trueAnalyse").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName = "/Users/marco/OfflineDocs/Wikipedia_Dump/finiti_copy"
    val errorFolderName  = "/Users/marco/OfflineDocs/Wikipedia_Dump/finiti_copy/error"
    //val sizeFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\size"
    val folderSeparator   = "/"

    //dataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(inputFolderName), "en", sparkSession)


    val startTime = System.currentTimeMillis()

    val sum_ = udf((xs: WA[Int]) => xs.sum)


    //def geoEncode(level: Int) = udf( (lat: Double, long: Double) => GeoHex.encode(lat, long, level)) df.withColumn("code", geoEncode(9)($"resolved_lat", $"resolved_lon")).show

    // Somma visualizzazioni anno
    val minMax = dataFrameSrc.withColumn("sum", sum_($"num_visualiz_anno")).sort(desc("sum"))


    val max = minMax.first().getAs[Int](7)

    val score_ = udf((xs: Int) => xs * 100.toDouble / max )

    //dataframe con score
    var scoreDF = minMax.withColumn("score",score_($"sum")).sort(desc("score"))

    // bonus pagina senza traduzione
    val translateBonus_ = udf((score: Double, transl: String) =>
        if (transl == "" ) score+20 else score
    )

    scoreDF = scoreDF.withColumn("score",translateBonus_($"score", $"id_pagina_tradotta")).sort(desc("score"))








    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}