import java.net.URLDecoder
import API._
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities._
import org.apache.spark.sql.functions.{col, collect_set, explode, sum, udf, when}
import prepareData.{compressRedirect, makeDimDF, missingIDsDF, removeErrorPages}

import scala.collection.mutable
import scala.collection.mutable.{WrappedArray => WA}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, udf}

import scala.collection.mutable.{WrappedArray => WA}
import scala.math._


object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
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

    val dataFrameSrc = sparkSession.read.parquet(bucket + args(1)).repartition(80)
    var dataFrameDst = sparkSession.read.parquet(bucket + args(2)).repartition(8)

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
    var dataFrameSize = makeDimDF(resultDataFrameSrc, resultDataFrameDst, sparkSession)



    // Inizio anaalisi

    val sumLong_ = udf((xs: WA[Long]) => xs.sum.toInt)
    val sumInt_ = udf((xs: WA[Int]) => xs.sum)

    // Somma visualizzazioni anno
    val minMaxSrc = resultDataFrameSrc.withColumn("sum", sumLong_($"num_visualiz_anno")).sort(desc("sum"))
    val minMaxDst = resultDataFrameDst.withColumn("sum", sumInt_($"num_visualiz_anno")).sort(desc("sum"))

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
    //scoreDF.show(20,false)


    scoreDF.select("id", "score").coalesce(1).write.csv(outputFolderName+"rankCSV")




    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}