
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

//case class perché sono immutabili
case class Entry(id: String, val1: String, val2: String)

object prepareData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("prepareData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val inputFile = "C:\\Users\\nik_9\\Desktop\\prova\\indice.txt"
    val outputFile = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    val input:org.apache.spark.rdd.RDD[String] = sparkContext.textFile(inputFile)

    //FileUtils.deleteDirectory(new File(outputFile))

    //result:org.apache.spark.rdd.RDD[Tupla]
    val result = input.map(line => {

      var result1:scalaj.http.HttpResponse[String] = APILangLinks.callAPI(line)
      //println(result1.body)

      var result2:scalaj.http.HttpResponse[String] = APIPageView.callAPI(line)
      //println(result2.body)

      Entry(line, result1.body, result2.body)
    })

    //TODO: da mettere le etichette giuste
    val resultDataFrame = result.toDF("id", "value1", "value2")

    resultDataFrame.show()

    //ferma anche lo sparkContext
    sparkSession.stop()
  }
}
