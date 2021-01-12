
import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import scalaj.http.Http

package API {

  object APILangLinks {
    def callAPI(url: String, lang:String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null

      println(url + " pt1")
      result = Http("https://" + URLEncoder.encode(lang, StandardCharsets.UTF_8) +
        ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&format=json&prop=langlinks"
      ).asString

      this.parseJSON(result.body)
    }

    def parseJSON(response: String): (Int, String) = {
      val json = ujson.read(response)
      val dati = json("parse").obj("langlinks").arr
      (dati.size, dati.map(item => if(item.obj("lang").str == "it" ) item.obj("url").str).filter(_ != ()).mkString("").replace("https://it.wikipedia.org/wiki/",""))

    }
  }

  object APIRedirect {
    def callAPI(url: String, lang:String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null

      println(url + " pt3")
      result = Http("https://" + URLEncoder.encode(lang, StandardCharsets.UTF_8) +
        ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&prop=text&format=json").asString
      this.parseJSON(result.body)
    }

    def parseJSON(response: String): (Int, String) = {
      val json = ujson.read(response)
      val text = json("parse").obj("text").obj("*").str
      val di_ref=if(text contains "class=\"redirectText\"" ) URLDecoder.decode(text.split("class=\"redirectText\"")(1).split("href=\"/wiki/")(1).split("\" title=\"")(0), StandardCharsets.UTF_8) else ""

      (text.getBytes.length, di_ref)
    }
  }

  object APIPageView {
    def callAPI(url: String, lang:String): (List[Int], List[Int]) = {
      var result:scalaj.http.HttpResponse[String] = null

      println(url + " pt2")
      result = Http("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/" + URLEncoder.encode(lang, StandardCharsets.UTF_8) +
        ".wikipedia/all-access/all-agents/" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "/monthly/20200101/20210101").asString

      if(result.is2xx) {
        this.parseJSON(result.body)
      } else{
        println(result.body)
        (List(0,0,0), List(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      }
    }

    def parseJSON(response: String): (List[Int], List[Int]) = {
      val json = ujson.read(response)
      //val views: List[Int] = json("items").arr.map(item => item.obj("views").toString.toInt).toList
      val mappa=json("items").arr.map(item => item.obj("timestamp").str.dropRight(4) -> item.obj("views").toString.toInt).toMap
      val anni = List("2018", "2019", "2020")
      val filtrato = anni.map(anno => mappa.filterKeys(_.dropRight(2) contains anno).values.sum)
      var mesi = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      mappa.filterKeys(_.dropRight(2) contains "2020").foreach(item => mesi(item._1.slice(4,6).toInt-1) = item._2)
      (filtrato, mesi.toList)
    }

  }

}