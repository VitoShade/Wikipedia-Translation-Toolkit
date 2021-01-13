
import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import scalaj.http.Http

package API {

  object APILangLinks {
    def callAPI(url: String, sourceLang: String, destLang: String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null
      var cond = true
      var ret:(Int, String) = null
      //println(url + " pt1")
      while(cond) {
        result = Http("https://" + URLEncoder.encode(sourceLang, StandardCharsets.UTF_8) +
          ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&format=json&prop=langlinks"
        ).asString
        if(result.is2xx) {
          cond = false
          try {
            ret = this.parseJSON(result.body, destLang)
          }catch{
            case e:Exception => ret = (0, "")
          }
        }
      }
      ret
    }

    def parseJSON(response: String, lang: String): (Int, String) = {
      val json = ujson.read(response)
      val dati = json("parse").obj("langlinks").arr
      (dati.size, dati.map(item => if(item.obj("lang").str == lang ) item.obj("url").str).filter(_ != ()).mkString("").replace("https://" + lang + ".wikipedia.org/wiki/",""))

    }
  }

  object APIRedirect {
    def callAPI(url: String, lang:String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null
      var cond = true
      var ret:(Int, String) = null
      //println(url + " pt3")
      while(cond) {
        result = Http("https://" + URLEncoder.encode(lang, StandardCharsets.UTF_8) +
          ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "&prop=text&format=json").asString
        if(result.is2xx) {
          cond = false
          try {
            ret = this.parseJSON(result.body)
          }catch{
            case e:Exception => ret = (0, "")
          }
        }
      }
      ret
    }

    def parseJSON(response: String): (Int, String) = {
      val json = ujson.read(response)
      val text = json("parse").obj("text").obj("*").str
      val di_ref=if(text contains "class=\"redirectText\"" ) URLDecoder.decode(text.split("class=\"redirectText\"")(1).split("href=\"/wiki/")(1).split("\" title=\"")(0), StandardCharsets.UTF_8) else ""

      (text.getBytes.length, di_ref)
    }
  }

  //Se lo mettiamo qui rischiamo di andare in loop infinito perchè se la pagina non esisteva in quel lasso di tempo, allora da 404
  object APIPageView {

    var lista_errori: Vector[(String, String)] = Vector()

    def callAPI(url: String, lang:String): (List[Int], List[Int]) = {
      var result:scalaj.http.HttpResponse[String] = null

      //println(url + " pt2")
      result = Http("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/" + URLEncoder.encode(lang, StandardCharsets.UTF_8) +
        ".wikipedia/all-access/all-agents/" + URLEncoder.encode(url, StandardCharsets.UTF_8) + "/monthly/20180101/20210101").asString

      if(result.is2xx) {
        this.parseJSON(result.body)
      } else{
        this.lista_errori=this.lista_errori :+  (url, result.body)
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