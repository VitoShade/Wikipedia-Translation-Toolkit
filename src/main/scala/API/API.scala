
import java.net._
import java.nio.charset.StandardCharsets
import scalaj.http.Http

package API {

  object APILangLinks {

    var lista_errori: Vector[(String, Vector[(Int, String)])] = Vector()

    def obtainErrorID(): Vector[String] = {
      this.lista_errori.map(el => el._1)
    }

    def obtainErrorDetails(): Vector[(String, Vector[(Int, String)])] = {
      this.lista_errori
    }

    def resetErrorList(): Unit = {
      this.lista_errori = Vector()
    }

    def callAPI(url: String, sourceLang: String, destLang: String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null
      var cond = false
      var ret:(Int, String) = (0, "")
      var counter: Int = 0
      var lista_errori_tmp: Vector[(Int, String)] = Vector()
      //println(url + " pt1")
      while(!cond && (counter < 10)) {
        try {
          result = Http("https://" + URLEncoder.encode(sourceLang, "UTF-8") +
            ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, "UTF-8") + "&format=json&prop=langlinks"
          ).asString
          if (result.is2xx) {
            try {
              ret = this.parseJSON(result.body, destLang)
              cond = true
            } catch {
              case e: Exception => lista_errori_tmp = lista_errori_tmp :+  (counter, e.getMessage)
            }
          }else{
            lista_errori_tmp = lista_errori_tmp :+  (counter, "codice risposta diverso da 2xx")

            if(!cond){
              Thread.sleep(500)
            }
          }
        }catch{
          case _:javax.net.ssl.SSLException => lista_errori_tmp = lista_errori_tmp :+  (counter, "BUG delle JDK 11")
          case e:Exception => lista_errori_tmp =  lista_errori_tmp :+  (counter, e.getMessage)
        }
        counter = counter + 1
      }
      if(!cond){
        this.lista_errori = this.lista_errori :+ (url, lista_errori_tmp)
      }
      ret
    }

    def parseJSON(response: String, lang: String): (Int, String) = {
      val json = ujson.read(response)
      val dati = json("parse").obj("langlinks").arr
      (dati.size, dati.map(item => if(item.obj("lang").str == lang ) item.obj("url").str).filter(_ != ()).mkString("").replace("https://" + lang + ".wikipedia.org/wiki/","").split("#")(0))

    }
  }

  object APIRedirect {

    var lista_errori: Vector[(String, Vector[(Int, String)])] = Vector()

    def obtainErrorID(): Vector[String] = {
      this.lista_errori.map(el => el._1)
    }

    def obtainErrorDetails(): Vector[(String, Vector[(Int, String)])] = {
      this.lista_errori
    }

    def resetErrorList(): Unit = {
      this.lista_errori = Vector()
    }

    def callAPI(url: String, lang:String): (Int, String) = {
      var result:scalaj.http.HttpResponse[String] = null
      var cond = false
      var ret:(Int, String) = (0, "")
      var counter: Int = 0
      var lista_errori_tmp: Vector[(Int, String)] = Vector()
      //println(url + " pt3")
      while(!cond && (counter <10)) {
        try {
          result = Http("https://" + URLEncoder.encode(lang, "UTF-8") +
            ".wikipedia.org/w/api.php?action=parse&page=" + URLEncoder.encode(url, "UTF-8") + "&prop=text&format=json").asString
          if(result.is2xx) {
            try {
              ret = this.parseJSON(result.body)
              cond = true
            }catch{
              case _:java.util.NoSuchElementException =>  lista_errori_tmp = lista_errori_tmp :+  (counter, "Pagina non trovata")
              case e:Exception => lista_errori_tmp =  lista_errori_tmp :+  (counter, e.getMessage)
            }
          }
          else{
            lista_errori_tmp = lista_errori_tmp :+  (counter, "codice risposta diverso da 2xx")

            if(!cond){
              Thread.sleep(500)
            }
          }
        }catch{
          case _:javax.net.ssl.SSLException => lista_errori_tmp = lista_errori_tmp :+  (counter, "BUG delle JDK 11")
          case e:Exception => lista_errori_tmp = lista_errori_tmp :+  (counter, e.getMessage)
        }
        counter = counter + 1
      }
      if(!cond){
        this.lista_errori = this.lista_errori :+ (url, lista_errori_tmp)
      }
      ret
    }

    def parseJSON(response: String): (Int, String) = {
      val json = ujson.read(response)
      val text = json("parse").obj("text").obj("*").str
      val di_ref=if(text contains "class=\"redirectText\"" ) URLDecoder.decode(text.split("class=\"redirectText\"")(1).split("href=\"/wiki/")(1).split("\" title=\"")(0).split("#")(0), "UTF-8") else ""

      (text.getBytes.length, di_ref)
    }
  }

  object APIPageView {

    var lista_errori: Vector[(String, Vector[(Int, String)])] = Vector()

    def obtainErrorID(): Vector[String] = {
      this.lista_errori.map(el => el._1)
    }

    def obtainErrorDetails(): Vector[(String, Vector[(Int, String)])] = {
      this.lista_errori
    }

    def resetErrorList(): Unit = {
      this.lista_errori = Vector()
    }

    def callAPI(url: String, lang:String): (Array[Int], Array[Int]) = {
      var result:scalaj.http.HttpResponse[String] = null
      var cond = false
      var ret:(Array[Int], Array[Int]) = (Array(0, 0, 0), Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
      var counter: Int = 0
      var lista_errori_tmp: Vector[(Int, String)] = Vector()
      //println(url + " pt2")
      while(!cond && (counter < 10)) {
        try {
          result = Http("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/" + URLEncoder.encode(lang, "UTF-8") +
            ".wikipedia/all-access/all-agents/" + URLEncoder.encode(url, "UTF-8") + "/monthly/20180101/20210101").asString

          if (result.is2xx) {
            ret = this.parseJSON(result.body)
            cond = true
          } else {
            lista_errori_tmp = lista_errori_tmp :+  (counter, "codice risposta diverso da 2xx")

            if(!cond){
              Thread.sleep(500)
            }
          }
        } catch {
          case e: javax.net.ssl.SSLException => lista_errori_tmp = lista_errori_tmp :+  (counter, e.getMessage)
          case e: Exception => lista_errori_tmp = lista_errori_tmp :+  (counter, e.getMessage)
        }
        counter = counter + 1
      }
      if(!cond){
        this.lista_errori = this.lista_errori :+ (url, lista_errori_tmp)
      }
      ret
    }

    def parseJSON(response: String): (Array[Int], Array[Int]) = {
      val json = ujson.read(response)
      //val views: List[Int] = json("items").arr.map(item => item.obj("views").toString.toInt).toList
      val mappa=json("items").arr.map(item => item.obj("timestamp").str.dropRight(4) -> item.obj("views").toString.toInt).toMap
      val anni = List("2018", "2019", "2020")
      val filtrato = anni.map(anno => mappa.filterKeys(_.dropRight(2) contains anno).values.sum)
      var mesi = Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      mappa.filterKeys(_.dropRight(2) contains "2020").foreach(item => mesi(item._1.slice(4,6).toInt-1) = item._2)
      (filtrato.toArray, mesi)
    }

  }

}