import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.{WrappedArray => WA}
import org.apache.spark.sql.functions.udf
import scala.math._

object analyseData extends App {

  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[32]").appName("analyseData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val inputFolderName = "/Users/marco/IdeaProjects/Wikipedia-Translation-Toolkit/file/outputProcessati"
    //val sizeFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\datiFinali\\size"
    val folderSeparator   = "/"

    //dataFrame dai parquet inglesi
    val dataFrameSrc = DataFrameUtility.dataFrameFromFoldersRecursively(Array(inputFolderName), "en", sparkSession)

    val startTime = System.currentTimeMillis()

    //DF standard
    //"id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect"


    // DF dimPages
    //"id", "id_pagina_tradotta",  "byte_dim_page", "id_traduzioni_redirect" = "id_redirect", ("id_ita", "byte_dim_page_ita_original), byte_dim_page_tot=sum byte dim page"



    val sum_ = udf((xs: WA[Int]) => xs.sum)


    //def geoEncode(level: Int) = udf( (lat: Double, long: Double) => GeoHex.encode(lat, long, level)) df.withColumn("code", geoEncode(9)($"resolved_lat", $"resolved_lon")).show

    // Somma visualizzazioni anno
    val minMax = dataFrameSrc.withColumn("sum", sum_($"num_visualiz_anno")).sort(desc("sum"))


    val max = minMax.first().getAs[Int](7)

    val score_ = udf((xs: Int) => xs * 100.toDouble / max )

    //dataframe con score
    var scoreDF = minMax.withColumn("score",score_($"sum")).sort(desc("score"))
    scoreDF.show(20, false)

    // bonus pagina senza traduzione linkate correttamente

    val translateBonus_ = udf((score: Double, idIta: String, singleEn: Int, sumEn: Int, byteIt:Int) => {
      var byteEn = 0
      if(idIta.isEmpty) byteEn = singleEn
      else byteEn = sumEn
      score + 20.0 * ((byteEn - byteIt).toDouble/ math.max(byteEn, byteIt))
    })

    //Aggiungiamo le pagine con link rotti

    scoreDF = scoreDF.withColumn("score",translateBonus_($"score", $"id_pagina_tradotta")).sort(desc("score"))


    // Bonus e malus per Anni e per Mesi

    val growingYearBonuses_ = udf((score: Double, xs: WA[Int]) => {
      val delta1 = (xs(1) - xs(0)).toDouble /math.max(xs(0),1)
      val delta2 = (xs(2) - xs(1)).toDouble /math.max(xs(0),1)
      //tarare le costanti
      (tanh(delta1)*6)+(tanh(delta2)*6)+score
    })

    scoreDF = scoreDF.withColumn("score",growingYearBonuses_($"score", $"num_visualiz_anno")).sort(desc("score"))
    //scoreDF.show(20, false)

    val growingMonthBonuses_ = udf((score: Double, xs: WA[Int]) => {
      val delta = (0 to 2).map( i => {
        xs(i*4)+xs(i*4+1)+xs(i*4+2)+xs(i*4+3)
      })
      val delta1 = (delta(1) - delta(0)).toDouble /math.max(delta(0),1)
      val delta2 = (delta(2) - delta(1)).toDouble /math.max(delta(0),1)
      //tarare le costanti
      (tanh(delta1)*2)+(tanh(delta2)*2)+score
    })

    scoreDF = scoreDF.withColumn("score",growingMonthBonuses_($"score", $"num_visualiz_mesi")).sort(desc("score"))
    //scoreDF.show(20, false)




    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}