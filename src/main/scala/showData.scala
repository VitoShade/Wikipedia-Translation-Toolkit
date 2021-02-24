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

    val nFile = args.drop(1).length
    val bucket = args(0)

    val errorFolderName   = bucket + "error/"
    val outputFolderName  = bucket + "datiFinali/"
    val sizeFolderName    = bucket + "datiFinali/size/"

    var dataFrameSrc = sparkSession.read.parquet(bucket + args(1)).repartition(80)
    var dataFrameDst = sparkSession.read.parquet(bucket + args(2)).repartition(8)

    val errorPagesSrc = sparkSession.read.textFile(errorFolderName + "errors.txt").toDF("id2")
    val errorPagesDst = sparkSession.read.textFile(errorFolderName + "errorsTranslated.txt").toDF("id2")

    sparkContext.getConf.getAll.foreach(println)

    //res.show(false)

    //println(res.count())

    //res.coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\nuovaCartella\\en")


    //val in = sparkSession.read.load("s3n://wtt-s3-1/indici/file53.txt")

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}
