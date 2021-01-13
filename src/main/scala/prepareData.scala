
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

    val inputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\indici"
    val tempFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\tempResult"
    val outputFolderName = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    val inputFolder = new File(inputFolderName)

    val files: Array[String] = inputFolder.listFiles.filter(file => file.isFile && (file.toString.takeRight(4) == ".txt")).map(file => file.toString)

    var outputFolders: Array[String] = Array[String]()

    //files.foreach(println)
    files.foreach(inputFileName => {
      val input: org.apache.spark.rdd.RDD[String] = sparkContext.textFile(inputFileName)

      val result = input.map(line => {

        //println(line)

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
      }).persist

      //println(result)

      val resultDataFrame = result.toDF("id", "num_traduzioni", "id_pagina_italiana", "num_visualiz_anno", "num_visualiz_mesi", "byte_dim_page", "id_redirect")

      //resultDataFrame.show(false)

      val outputName = inputFileName.drop(36).dropRight(4)
      val outputFolder = tempFolderName + "\\" + outputName

      outputFolders = outputFolders :+ outputFolder

      //println(outputFolder)

      FileUtils.deleteDirectory(new File(outputFolder))
      resultDataFrame.write.parquet(outputFolder)

    })

    //outputFolders.foreach(println)

    var allFiles: Array[String] = Array[String]()

    outputFolders.foreach(tempFolder => {
        val folder = new File(tempFolder)

        val files: Array[String] = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file toString)

        allFiles = allFiles ++ files

      }
    )

    //allFiles.foreach(println)

    val dataFrameFromFiles = allFiles map (n => sparkSession.read.parquet(n))

    //dataFrameFromFiles.foreach(_.show(false))

    val res2 = dataFrameFromFiles.reduce(_ union _)

    res2.show(false)

    FileUtils.deleteDirectory(new File(outputFolderName))
    res2.write.parquet(outputFolderName)


    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
