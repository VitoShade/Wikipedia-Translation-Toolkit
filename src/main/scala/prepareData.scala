
import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView
import org.apache.commons.io.FileUtils
import java.io._

//case class perché sono immutabili
case class Entry(id: String,
                 numTraduzioni: Int,
                 IDPaginaIta: String,
                 numVisualizzazioniAnno: List[Int],
                 numVisualizzazioniMesi: List[Int],
                 numByte: Int,
                 IDPaginaPrincipale: String)

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFile = "C:\\Users\\nik_9\\Desktop\\prova\\indice10.txt"
    val outputFile = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    val input:org.apache.spark.rdd.RDD[String] = sparkContext.textFile(inputFile)

    val result = input.map(line => {

      var tuple1 = APILangLinks.callAPI(line, "en", "it")
      val num_traduzioni = tuple1._1
      val id_pagina_italiana = tuple1._2
      //println(tuple1)

      var tuple2 = APIPageView.callAPI(line, "en")
      val num_visualiz_anno = tuple2._1
      val num_visualiz_mesi = tuple2._2
      //println(line + "   " + tuple2)
      //println(num_visualiz_anno)

      var tuple3 = APIRedirect.callAPI(line, "en")
      val byte_dim_page = tuple3._1
      val id_redirect = tuple3._2
      //println(result3)

      Entry(line, num_traduzioni, id_pagina_italiana, num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)
    }).persist()

    //println(result)

    val resultDataFrame = result.toDF("id", "num_traduzioni", "id_pagina_italiana", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

    //resultDataFrame.show(false)

    FileUtils.deleteDirectory(new File(outputFile))
    resultDataFrame.write.parquet(outputFile)

    val folder = new File(outputFile)

    val files: Array[String] = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file toString)

    val dataFrameFromFiles = files map (n => sparkSession.read.parquet(n))

    dataFrameFromFiles.foreach(_.show(false))

    val res2 = dataFrameFromFiles.reduce(_ union _)

    res2.show(false)

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
