import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.{WrappedArray => WA}
import org.apache.spark.sql.functions.udf
import scala.math._

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("analyseData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali"
    val errorFolderName  = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\error"
    val sizeFolderName   = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\size"
    val folderSeparator = "\\"

    //dataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(inputFolderName), "en", sparkSession)

    val startTime = System.currentTimeMillis()


    //"id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect"




    val sum_ = udf((xs: WA[Int]) => xs.sum)


    //def geoEncode(level: Int) = udf( (lat: Double, long: Double) => GeoHex.encode(lat, long, level)) df.withColumn("code", geoEncode(9)($"resolved_lat", $"resolved_lon")).show

    // Somma visualizzazioni anno
    val minMax = dataFrameSrc.withColumn("sum", sum_($"num_visualiz_anno")).sort(desc("sum"))


    val max = minMax.first().getAs[Int](7)

    val score_ = udf((xs: Int) => xs * 100.toDouble / max )

    //dataframe con score
    var scoreDF = minMax.withColumn("score",score_($"sum")).sort(desc("score"))

    // bonus pagina senza traduzione
    val translateBonus_ = udf((score: Double, byteIt: Int, byteEn: Int) =>
      // se somma traduzioni inglese = null
      //      byteEn = byte_dim_page
      // else
      //      byteEn = sum_byte_en
      // byteIt = byte_pagina_tradotta + sum(byte traduzioni redirect)
      score + 20.0 * ((byteEn - byteIt).toDouble/ math.max(byteEn, byteIt))
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