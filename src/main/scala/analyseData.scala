
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

    /*val allInputFolders = DataFrameUtility.collectParquetFilesFromFolders(inputFolders)

    val dataFrameFilesSrc = allInputFolders map (tempFile => sparkSession.read.parquet(tempFile))

    val dataFrameSrc = dataFrameFilesSrc.reduce(_ union _)*/

    //dataFrameSrc.show(false)

    //val inputRDD = dataFrameSrc.rdd

    /*val dataFrameRedirect = dataFrameSrc.
      filter("id_redirect != ''").
      //select("num_visualiz_anno","num_visualiz_mesi","id_redirect")
      select("num_visualiz_anno","id_redirect")

    dataFrameRedirect.show(false)

    val prova = dataFrameRedirect.groupBy($"id_redirect").agg(Summarizer.sum($"num_visualiz_anno").alias("num_visualiz_anno"))

    prova.show()*/

    val df = sparkSession.createDataFrame(Seq(
      (1, org.apache.spark.ml.linalg.Vectors.dense(0,0,5)),
      (1, org.apache.spark.ml.linalg.Vectors.dense(4,0,1)),
      (1, org.apache.spark.ml.linalg.Vectors.dense(1,2,1)),
      (2, org.apache.spark.ml.linalg.Vectors.dense(7,5,0)),
      (2, org.apache.spark.ml.linalg.Vectors.dense(3,3,4)),
      (3, org.apache.spark.ml.linalg.Vectors.dense(0,8,1)),
      (3, org.apache.spark.ml.linalg.Vectors.dense(0,0,1)),
      (3, org.apache.spark.ml.linalg.Vectors.dense(7,7,7)))
    ).toDF("id", "vec")

    df.show(false)

    println(df)

    val res = df.groupBy($"id")
      .agg(Summarizer.sum($"vec").alias("vec"))

    res.show(false)



    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    //ferma anche lo sparkContext
    sparkSession.stop()
  }

}
