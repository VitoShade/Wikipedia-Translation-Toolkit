
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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
    val conf = new SparkConf().setAppName("pageRank").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val inputFile = "C:\\Users\\nik_9\\Desktop\\prova\\indice.txt"
    val outputFile = "C:\\Users\\nik_9\\Desktop\\prova\\result"

    val input:org.apache.spark.rdd.RDD[String] = sc.textFile(inputFile)

    //FileUtils.deleteDirectory(new File(outputFile))

    //val numWorkers = sc.statusTracker.getExecutorInfos

    //println(numWorkers.length)

    /*def currentActiveExecutors(sc: SparkContext): Seq[String] = {
      val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
      val driverHost: String = sc.getConf.get("spark.driver.host")
      allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
    }*/

    //println(sc.getConf.getInt("spark.executor.instances", 3))


    input.map(line => {

      var result1:scalaj.http.HttpResponse[String] = APILangLinks.callAPI(line)
      println(result1.body)

      var result2:scalaj.http.HttpResponse[String] = APIPageView.callAPI(line)
      println(result2.body)

      //result.saveAsTextFile(outputFile)

    }).count()



    //    FileUtils.deleteDirectory(new File(outputFile))
    //    res.saveAsTextFile(outputFile)

    /*val inputFile = "C:\\Users\\nik_9\\Desktop\\soc_Epinions.txt"
    val outputFile = "C:\\Users\\nik_9\\Desktop\\pageRank"
    val input = sc.textFile(inputFile)
    val edges = input.map(s => (s.split("\t"))).
      map(a => (a(0).toInt,a(1).toInt))
    val links = edges.groupByKey().
      partitionBy(new HashPartitioner(4)).persist()
    var ranks = links.mapValues(v => 1.0)
    for(i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (u, (uLinks, rank)) =>
          uLinks.map(t => (t, rank / uLinks.size))
      }
      ranks = contributions.reduceByKey((x,y) => x+y).
        mapValues(v => 0.15+0.85*v)
    }
    FileUtils.deleteDirectory(new File(outputFile))
    ranks.saveAsTextFile(outputFile)*/
    sc.stop()
  }
}

/*object QuickSort extends App {

  println("ciao")

  val result = Http("https://en.wikipedia.org/w/api.php?action=parse&page=COVID-19_pandemic&format=json&prop=langlinks").asString

  //println(result.body)
  //println(result.headers)

  /*val source = """{ "some": "JSON source" }"""
  val jsonAst = source.parseJson // or JsonParser(source)*/

  //println(jsonAst)

  println("fine")

}*/