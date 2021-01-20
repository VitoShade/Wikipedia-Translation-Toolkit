
import org.apache.spark.sql.{Row, SparkSession}
import API._
import Utilities._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{concat, desc, length}
import java.io._
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.spark.ml.stat.Summarizer

object analyseData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[25]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    //val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati\\File1-3"
    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    /*val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"
    val errorFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result\\error"*/
    val folderSeparator = "\\"

    val inputFolders = Array(inputFolderName + folderSeparator + "en")

    val allInputFolders = DataFrameUtility.collectParquetFilesFromFolders(inputFolders)

    val dataFrameFilesSrc = allInputFolders map (tempFile => sparkSession.read.parquet(tempFile))

    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)

    //dataFrameSrc.show(false)

    //val inputRDD = dataFrameSrc.rdd

    //dataFrameSrc.select("num_visualiz_anno","num_visualiz_mesi","id_redirect").groupBy("id_redirect").count().show(false)
    /*dataFrameSrc.
      filter("id_redirect != ''").
      select("num_visualiz_anno","num_visualiz_mesi","id_redirect").
      groupBy("id_redirect").
      count().
      show(false)*/

    val dataFrameRedirect = dataFrameSrc.
      filter("id_redirect != ''").
      //select("num_visualiz_anno","num_visualiz_mesi","id_redirect")
      select("num_visualiz_anno","id_redirect")

    dataFrameRedirect.show(false)

    val prova = dataFrameRedirect.groupBy("id_redirect").agg(Summarizer.sum($"num_visualiz_anno"))

    prova.show()

    //println(dataFrameRedirect.first().getAs("id_redirect"))
    //val res = dataFrameRedirect.reduce((x:Row("num_visualiz_anno","id_redirect"),y:Row)=>Row(x.getAs("id_redirect"), x.getAs("num_visualiz_anno")))
    //val res = dataFrameRedirect.groupBy("id_redirect").agg(($"num_visualiz_anno"))

    //println(res)


    //dataFrameRedirect.withColumn()

    //dataFrameSrc.select("num_visualiz_anno","num_visualiz_mesi","id_redirect").orderBy(desc("id_redirect")).show(false)



    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

}
