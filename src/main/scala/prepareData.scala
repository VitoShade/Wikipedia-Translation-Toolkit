
import org.apache.spark.sql.SparkSession
import API.APILangLinks
import API.APIRedirect
import API.APIPageView

//case class perché sono immutabili
case class Entry(id: String, val1: String, val2: String)

object prepareData extends App {



  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFile = "/Users/marco/Desktop/indice.txt"
    val outputFile = "/Users/marco/Desktop/result.txt"

    val input:org.apache.spark.rdd.RDD[String] = sparkContext.textFile(inputFile)

    //FileUtils.deleteDirectory(new File(outputFile))

    //:org.apache.spark.rdd.RDD[(String, String, String)]
    val result = input.map(line => {

      //var result1:scalaj.http.HttpResponse[String] = APILangLinks.callAPI(line)
      //println(result1.body)
      var tuple1 = APILangLinks.callAPI(line, "en")
      val num_traduzioni = tuple1._1
      val id_pagina_italiana = tuple1._2
      //println(tuple1)
      var tuple2 = APIPageView.callAPI(line, "en")
      val num_visualiz_anno = tuple2._1
      val num_visualiz_mesi = tuple2._2
      //println(line + "   " + tuple2)
      APIPageView.callAPI(line, "en")
      var tuple3 = APIRedirect.callAPI(line, "en")
      val byte_dim_page = tuple3._1
      val id_redirect = tuple3._2
      //println(result3)
      (line, num_traduzioni, id_pagina_italiana, num_visualiz_anno, num_visualiz_mesi, byte_dim_page, id_redirect)
    }).count()

    //TODO: da mettere le etichette giuste
    //val resultDataFrame = result.toDF("id", "value1", "value2", "value3")

    // Saving into an external file
    // resultDataFrame.write.save("dataset.parquet")

    //resultDataFrame.show()

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
