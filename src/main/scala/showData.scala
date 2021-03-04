
import API.{APILangLinks, APIPageView, APIRedirect}
import Utilities.DataFrameUtility
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, desc, udf}
import scala.collection.mutable.{WrappedArray => WA}
import prepareData.{compressRedirect, makeDimDF, missingIDsDF, removeErrorPages}

import java.net.URLDecoder
import scala.collection.mutable
import scala.math.tanh


object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    //val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    //per convertire RDD in DataFrame
    import sparkSession.implicits._

    val startTime = System.currentTimeMillis()

    /*val dataFrames = inputFile.map(line => {
        println(path + line)
        sparkSession.read.parquet(path + line)
      }
    )*/

    //merge dei parquet in un dataFrame unico
    /*
    val dataFrameTotal = dataFrames.reduce(_ union _)
    dataFrameTotal.coalesce(1).write.parquet("s3n://wtt-s3-1/provaOutput")
     */

    //DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\Nuova cartella"), "en", sparkSession).coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\unioneCompleta\\en")
    //DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\Nuova cartella"), "it", sparkSession).coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\unioneCompleta\\it")

    /*
        val dataEn = DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\in3M"), "en", sparkSession)
        val dataIt = DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\in3M"), "it", sparkSession)

        //println(dataEn.dropDuplicates().count())
        //println(dataIt.dropDuplicates().count())


        val resEn = dataEn.map(row => {

          var paginaDecodificata : String = row.getString(2)

          try {
            paginaDecodificata = URLDecoder.decode(row.getString(2), "UTF-8")
          } catch {
            case e : IllegalArgumentException => println(row.getString(2))
          }

          (row.getString(0), row.getInt(1), paginaDecodificata.split("#")(0), row.getAs[mutable.WrappedArray[Int]](3), row.getAs[mutable.WrappedArray[Int]](4), row.getInt(5), row.getString(6))
        }).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

        //resEn.show(false)

        //println(resEn.dropDuplicates().count())

        resEn.dropDuplicates().coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\3M\\en")

        val resIt = dataIt.map(row => {

          (row.getString(0).split("#")(0), row.getString(1).split("#")(0), row.getAs[mutable.WrappedArray[Int]](2), row.getAs[mutable.WrappedArray[Int]](3), row.getInt(4), row.getString(5))
        }).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

        //resIt.show(false)

        //println(resIt.dropDuplicates().count())

        resIt.dropDuplicates().coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\3M\\it")
    */






    //val err = sparkContext.textFile("C:\\Users\\nik_9\\Desktop\\prova\\datiUniti\\errors.txt").toDF("id2")
    //val en = sparkSession.read.parquet("C:\\Users\\nik_9\\Desktop\\prova\\datiUniti\\en\\part-00000-952fe5ad-ee2e-4de1-8330-efc31a2c1b54-c000.snappy.parquet")


    /*
    val filtro = udf((id: String, xs: mutable.WrappedArray[Int]) => {

      if(xs(0) == 0 || xs(1) == 0 || xs(2) == 0)
        id
      else ""

    })

    val ret = en.join(err, en("id") === err("id2"), "inner").withColumn("nuova", filtro($"id", $"num_visualiz_anno")).filter("nuova != ''").filter("id_redirect != '' AND id_pagina_tradotta != ''")
    ret.drop("id2", "nuova").show(false)

     */







    /*
        val nFile = args.drop(1).length
        val bucket = args(0)

        val errorFolderName   = bucket + "error\\"
        val outputFolderName  = bucket + "datiFinali\\"
        val sizeFolderName    = bucket + "datiFinali\\size\\"

        /*

        // Unione dei DataFrame dai parquet inglesi
        val dataFramesEn = args.slice(1, nFile/2+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
        var dataFrameSrc = dataFramesEn.reduce(_ union _)

        // Unione dei DataFrame italiani
        val dataFramesIt = args.slice(nFile/2+1, nFile+1) map (tempFile => sparkSession.read.parquet(bucket + tempFile))
        var dataFrameDst = dataFramesIt.reduce(_ union _)


         */

        var dataFrameSrc = sparkSession.read.parquet(bucket + args(1)).repartition(80)
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

        dataFrameSrc = resultDataFrameSrc
        dataFrameDst = resultDataFrameDst


        // Inizio anaalisi

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
        //scoreDF.show(20,false)


        scoreDF.select("id", "score").coalesce(1).write.csv(outputFolderName+"rankCSV")
     */

    val df1 = Seq(
      ("D", 2, "RB", Seq(10, 20, 240), Seq(240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 1000, ""),
      ("A", 2, "B", Seq(10, 20, 240), Seq(240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 3500, ""),
      ("R1A", 2, "B", Seq(10, 20, 240), Seq(240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 2000, "A"),
      ("R2A", 2, "RC", Seq(10, 20, 240), Seq(240, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 4000, "A")
    ).toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    val df2 = Seq(
      ("RB", "", Seq(10, 2, 30), Seq(15, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 444, "B"),
      ("RC", "", Seq(10, 2, 30), Seq(15, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 520, "C"),
      ("C", "", Seq(10, 2, 30), Seq(15, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 250, ""),
      ("B", "", Seq(10, 2, 30), Seq(15, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), 815, "")
    ).toDF("id", "id_pagina_originale", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    df1.show(false)
    df2.show(false)

    df1.coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\exp\\en")
    df2.coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\exp\\it")


    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}
