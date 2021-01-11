
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpConstants, HttpRequest}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

object APILangLinks {
  def callAPI(url: String): scalaj.http.HttpResponse[String] = {
    var result:scalaj.http.HttpResponse[String] = null

    println(url + " pt1")
    result = Http("https://en.wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&format=json&prop=langlinks").asString

    result
  }
}

object APIPageView {
  def callAPI(url: String): scalaj.http.HttpResponse[String] = {
    var result:scalaj.http.HttpResponse[String] = null

    println(url + " pt2")
    result = Http("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "/monthly/20200101/20200201").asString

    result
  }
}

object APILangLinksSync {
  def callAPI(url: String): scalaj.http.HttpResponse[String] = {
    var result:scalaj.http.HttpResponse[String] = null

    this.synchronized {
      println(url + " pt1")
      result = Http("https://en.wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&format=json&prop=langlinks").asString
    }

    result
  }
}

object APIPageViewSync {
  def callAPI(url: String): scalaj.http.HttpResponse[String] = {
    var result:scalaj.http.HttpResponse[String] = null

    this.synchronized {
      println(url + " pt2")
      result = Http("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "/monthly/20200101/20200201").asString
    }

    result
  }
}

object prepareData extends App {
  override def main(args: Array[String]) {
    /*val conf = new SparkConf().setAppName("pageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)*/

    val spark: SparkSession = SparkSession.builder.master("local[8]").getOrCreate
    val sc = spark.sparkContext

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val inputFile = "/Users/marco/Desktop/indice.txt"
    val outputFile = "/Users/marco/Desktop/result.txt"

    val input:org.apache.spark.rdd.RDD[String] = sc.textFile(inputFile)

    //FileUtils.deleteDirectory(new File(outputFile))

    //:org.apache.spark.rdd.RDD[(String, String, String)]
    val result = input.map(line => {

      var result1:scalaj.http.HttpResponse[String] = APILangLinks.callAPI(line)
      //println(result1.body)

      var result2:scalaj.http.HttpResponse[String] = APIPageView.callAPI(line)
      //println(result2.body)

      (line, result1.body, result2.body)
    })

    val resultDataFrame = result.toDF("id", "value1", "value2")

    resultDataFrame.show()

    sc.stop()
  }
}
