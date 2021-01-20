
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
                      numVisualizzazioniAnno: Vector,
                      numVisualizzazioniMesi: Vector
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
        org.apache.spark.ml.linalg.Vectors.dense(anniDouble),
        org.apache.spark.ml.linalg.Vectors.dense(mesiDouble)
      )
    })

    //creazione del dataFrame a partire dal dataSet
    val newDataFrameRedirect = dataFrameRedirectDouble.toDF("id_redirect", "num_visualiz_anno", "num_visualiz_mesi")

    newDataFrameRedirect.orderBy(asc("id_redirect")).filter("id_redirect == 'American_Indian_boarding_schools'").show(false)

    //somma degli array per righe con lo stesso id_redirect
    val summarizedDataFrameRedirect = newDataFrameRedirect.groupBy($"id_redirect").agg(
      Summarizer.sum($"num_visualiz_anno"),
      Summarizer.sum($"num_visualiz_mesi")
    )

    //prova.filter("id_redirect == 'American_Indian_boarding_schools'").show(false)List_of_Native_American_artists
    summarizedDataFrameRedirect.filter("id_redirect == 'American_Indian_boarding_schools'").show(false)




    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

}
