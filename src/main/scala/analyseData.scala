
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import API._
import Utilities._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{concat, desc, length}
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.linalg._
import scala.collection.mutable

case class EntryClean(id: String,
                      numTraduzioni: Int,
                      IDPaginaTradotta: String,
                      numVisualizzazioniAnno: Vector,
                      numVisualizzazioniMesi: Vector,
                      numByte: Int,
                    )

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[16]").appName("analyseData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati\\File1-3"
    //val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    /*val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result\\error"*/
    val folderSeparator = "\\"

    //cartella parquet inglesi
    val inputFolders = Array(inputFolderName + folderSeparator + "en")

    val allInputFolders = DataFrameUtility.collectParquetFilesFromFolders(inputFolders)

    val dataFrameFilesSrc = allInputFolders map (tempFile => sparkSession.read.parquet(tempFile))

    //merge dei parquet in un file unico
    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    //dataFrameSrc.show(false)

    //selezione delle righe che sono redirect
    val dataFrameRedirect = dataFrameSrc.
      filter("id_redirect != ''").
      select("num_visualiz_anno","num_visualiz_mesi","id_redirect")

    //dataFrameRedirect.show(false)

    //cast degli array del dataFrame da Int a Double
    val dataFrameRedirectDouble = dataFrameRedirect.map(row => {

      val anniInt = row.getAs[mutable.WrappedArray[Int]]("num_visualiz_anno").toArray
      val anniDouble = anniInt map (_.toDouble)

      val mesiInt = row.getAs[mutable.WrappedArray[Int]]("num_visualiz_mesi").toArray
      val mesiDouble = mesiInt map (_.toDouble)

      EntryClean(
        row.getAs("id_redirect"),
        0,
        "",
        org.apache.spark.ml.linalg.Vectors.dense(anniDouble),
        org.apache.spark.ml.linalg.Vectors.dense(mesiDouble),
        0
      )
    })

    //creazione del dataFrame a partire dal dataSet
    val newDataFrameRedirect = dataFrameRedirectDouble.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")

    //newDataFrameRedirect.orderBy(asc("id")).filter("id == 'American_Indian_boarding_schools'").show(false)
    //newDataFrameRedirect.orderBy(asc("id")).show(false)

    println("prima di riduzione " + newDataFrameRedirect.count())

    //somma degli array per righe con lo stesso id_redirect
    //per num_traduzioni, id_pagina_tradotta, byte_dim_page si può prendere un valore qualsiasi tra quelli disponibili, sono tutti 0 oppure ""
    val summarizedDataFrameRedirect = newDataFrameRedirect.groupBy($"id").agg(
      first($"num_traduzioni").as("num_traduzioni"),
      first($"id_pagina_tradotta").as("id_pagina_tradotta"),
      Summarizer.sum($"num_visualiz_anno").as("num_visualiz_anno"),
      Summarizer.sum($"num_visualiz_mesi").as("num_visualiz_mesi"),
      first($"byte_dim_page").as("byte_dim_page"),
    )

    //American_Indian_boarding_schools
    //List_of_Native_American_artists
    //prova.filter("id_redirect == ''").show(false)
    //summarizedDataFrameRedirect.filter("id == ''").show(false)
    //summarizedDataFrameRedirect.show(false)
    println("dopo riduzione " + summarizedDataFrameRedirect.count())


    //selezione delle righe delle pagine princiali (non redirect)
    val dataFramePrincipal = dataFrameSrc.
      filter("id_redirect == ''").
      select("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")

    //cast degli array del dataFrame da Int a Double
    val dataFramePrincipalDouble = dataFramePrincipal.map(row => {

      val anniInt = row.getAs[mutable.WrappedArray[Int]]("num_visualiz_anno").toArray
      val anniDouble = anniInt map (_.toDouble)

      val mesiInt = row.getAs[mutable.WrappedArray[Int]]("num_visualiz_mesi").toArray
      val mesiDouble = mesiInt map (_.toDouble)

      EntryClean(
        row.getAs("id"),
        row.getAs("num_traduzioni"),
        row.getAs("id_pagina_tradotta"),
        org.apache.spark.ml.linalg.Vectors.dense(anniDouble),
        org.apache.spark.ml.linalg.Vectors.dense(mesiDouble),
        row.getAs("byte_dim_page")
      )
    })

    //creazione del dataFrame a partire dal dataSet
    val newDataFramePrincipal = dataFramePrincipalDouble.toDF("id", "num_traduzioni", "id_pagina_tradotta", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page")

    //newDataFramePrincipal.show(false)

    val dataFrameResultPairs = newDataFramePrincipal.union(summarizedDataFrameRedirect)

    println("prima di riduzione " + dataFrameResultPairs.count())

    //Naukluft_Mountains
    //dataFrameResultPairs.filter("id_pagina_tradotta != ''").show(false)
    //dataFrameResultPairs.filter("id == ''").show(false)
    //dataFrameResultPairs.groupBy("id").count().filter("count > 1").show(50,false)

    //TODO: si cercano di unire coppie che potrebbero non essere nella stessa posizione del dataset, le pagine con dimensione 0 sono la somma delle redirect
    //somma degli array per righe con lo stesso id_redirect
    //per num_traduzioni, id_pagina_tradotta, byte_dim_page si prende il valore massimo perché gli altri sono 0 oppure ""
    val dataFrameResult = dataFrameResultPairs.groupBy($"id").agg(
      max($"num_traduzioni").as("num_traduzioni"),
      max($"id_pagina_tradotta").as("id_pagina_tradotta"),
      Summarizer.sum($"num_visualiz_anno").as("num_visualiz_anno"),
      Summarizer.sum($"num_visualiz_mesi").as("num_visualiz_mesi"),
      max($"byte_dim_page").as("byte_dim_page"),
    )

    println("dopo riduzione " + dataFrameResult.count())
    //dataFrameResult.filter("id == ''").show(false)
    //non vuote cioè pagine principali e non somme di pagine redirect
    println("dopo riduzione pagine non vuote " + dataFrameResult.filter("byte_dim_page > 0").count())



    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

}
