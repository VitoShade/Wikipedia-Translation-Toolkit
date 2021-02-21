import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import scala.collection.mutable.{WrappedArray => WA}
import org.apache.spark.sql.functions.udf
import scala.math._

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().appName("analyseData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val bucket = args(0)

    val outputFolderName  = bucket + "risultato/"

    val startTime = System.currentTimeMillis()

    //dataFrame dai parquet inglesi
    val dataFrameSrc = sparkSession.read.parquet(bucket + args(1))

    //dataFrame dai parquet italiani
    val dataFrameDst = sparkSession.read.parquet(bucket + args(2))

    //dataframe delle dimensioni
    var dataFrameSize = sparkSession.read.parquet(bucket + args(3))


    // DF standard
    //"id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_traduzioni_redirect"

    // DF italiano
    // //"id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect"

    // DF dimPages
    //"id", "byte_dim_page", "id_traduzioni_redirect", "id_traduzioni_redirect_dim" ("id_ita", "byte_dim_page_ita_original), byte_dim_page_tot=sum byte dim page"

    val sumLong_ = udf((xs: WA[Long]) => xs.sum.toInt)
    val sumInt_ = udf((xs: WA[Int]) => xs.sum)

    // Somma visualizzazioni anno
    val minMaxSrc = dataFrameSrc.withColumn("sum", sumLong_($"num_visualiz_anno")).sort(desc("sum"))
    val minMaxDst = dataFrameDst.withColumn("sum", sumInt_($"num_visualiz_anno")).sort(desc("sum"))

    val maxSrc = minMaxSrc.first().getAs[Int](7)
    val maxDst = minMaxDst.first().getAs[Int](6)

    def score_(max: Int) = udf((xs: Int) => xs * 100.toDouble / max )

    //dataframe con score
    var scoreDF = minMaxSrc.withColumn("score",score_(maxSrc)($"sum")).sort(desc("score"))

    var scoreDFDst = minMaxDst.withColumn("score",score_(maxDst)($"sum")).sort(desc("score"))

    // Crescita/decrescita per anni/mesi
    def growingYearBonuses_ = udf((score: Double, xs: WA[AnyVal]) => {

      val years = xs.map(xi => {xi.asInstanceOf[Number].longValue()})

      val delta1 = (years(1) - years(0)).toDouble / math.max(years(0),1)
      val delta2 = (years(2) - years(1)).toDouble / math.max(years(0),1)
      //tarare le costanti
      (tanh(delta1)*6)+(tanh(delta2)*8)+score
    })

    scoreDF = scoreDF.withColumn("score",growingYearBonuses_($"score", $"num_visualiz_anno")).sort(desc("score"))
    scoreDFDst = scoreDFDst.withColumn("score",growingYearBonuses_($"score", $"num_visualiz_anno")).sort(desc("score"))

    def growingMonthBonuses_ = udf((score: Double, xs: WA[AnyVal]) => {

      val months = xs.map(xi => {xi.asInstanceOf[Number].longValue()})

      val delta = (0 to 2).map( i => {
        months(i*4)+months(i*4+1)+months(i*4+2)+months(i*4+3)
      })

      val delta1 = (delta(1) - delta(0)).toDouble / math.max(delta(0),1)
      val delta2 = (delta(2) - delta(1)).toDouble / math.max(delta(0),1)
      //tarare le costanti
      (tanh(delta1)*2)+(tanh(delta2)*3)+score
    })

    scoreDF = scoreDF.withColumn("score",growingMonthBonuses_($"score", $"num_visualiz_mesi")).sort(desc("score"))

    scoreDFDst = scoreDFDst.withColumn("score",growingMonthBonuses_($"score", $"num_visualiz_mesi")).sort(desc("score"))

    val maxScoreSrc = scoreDF.first().getAs[Double](8)
    val maxScoreDst = scoreDFDst.first().getAs[Double](7)
    val maxScore = maxScoreSrc + maxScoreDst

    scoreDFDst = scoreDFDst.withColumnRenamed("score","scoreIta").drop("id")
    scoreDF = scoreDF.join(scoreDFDst.select("id_pagina_originale","scoreIta"), scoreDF("id") === scoreDFDst("id_pagina_originale"),"left_outer")
      .na.fill("", Seq("id_pagina_originale"))
      .na.fill(0, Seq("scoreIta"))
      .sort(desc("score"))


    //riuniune score su tabella inglese

    val sumMean_ = udf((score: Double, idIta : String, scoreIta:Double) => {
      if (idIta.isEmpty) score
      else 100*(score+(0.5*scoreIta)) / maxScore
    })

    scoreDF = scoreDF.withColumn("score", sumMean_($"score", $"id_pagina_tradotta", $"scoreIta")).sort(desc("score"))
    scoreDF = scoreDF.drop("sum","scoreIta","id_pagina_tradotta")

    //dimensioni

    dataFrameSize = dataFrameSize.join(scoreDF.select("id","score"),Seq("id")).sort(desc("score"))

    // bonus pagina senza traduzione linkate correttamente

    val translateBonus_ = udf((score: Double, idIta: String, singleEn: Int, sumEn: Int, singleIt:Int, redirectDim:Int ) => {
      val byteEn = if(idIta.isEmpty) singleEn else sumEn
      val byteIt = singleIt + redirectDim
      val bonus  = if(idIta.isEmpty) 8.0 else 0.0

      score + bonus + 25.0 * ((byteEn - byteIt).toDouble / math.max(byteEn, byteIt))
    })

    //Aggiungiamo le pagine con link rotti
    dataFrameSize = dataFrameSize.withColumn("score", translateBonus_($"score", $"id_ita", $"byte_dim_page", $"byte_dim_page_tot", $"byte_dim_page_ita_original", $"id_traduzioni_redirect_dim")).sort(desc("score"))

    // riporto lo score
    scoreDF = scoreDF.drop("score").join(dataFrameSize.select("id", "score"), Seq("id")).sort(desc("score"))
    scoreDF.show(20,false)

    scoreDF.coalesce(1).write.parquet(outputFolderName+"rankPARQ")

    scoreDF.select("id", "score").rdd.coalesce(1).saveAsTextFile(outputFolderName+"rankTXT")
    scoreDF.select("id", "score").coalesce(1).write.csv(outputFolderName+"rankCSV")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}