
import java.io._
import scala.collection.mutable
import org.apache.spark.sql.SparkSession

package Utilities {

  import org.apache.spark.sql.Dataset

  object DataFrameUtility {

    def collectParquetFilesFromFolders(folders: Array[String]): Array[String] = {

      var allParquetFiles = Array[String]()

      folders.foreach(folderName => {

        val folder = new File(folderName)

        val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

        allParquetFiles = allParquetFiles ++ files

      })

      allParquetFiles
    }

    def collectParquetFromFoldersRecursively(folders: Array[String], subFolder: String): Array[String] = {

      var allParquetFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(2) == subFolder) {

          val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

          allParquetFiles = allParquetFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      allParquetFiles
    }

    def collectErrorPagesFromFoldersRecursively(folders: Array[String], sparkSession: SparkSession, translated: Boolean): Dataset[String] = {

      var errorFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      val tr: String = if(translated) "Translated" else ""

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(5) == "error") {

          val files = folder.listFiles.filter(file => file.isFile && (
            file.toString.contains("errorLangLinks"+tr+".txt") || file.toString.contains("errorRedirect"+tr+".txt") || file.toString.contains("errorView"+tr+".txt")
            )).map(file => file.toString)

          errorFiles = errorFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val wikiPagesWithErrorRepetitions = errorFiles.map(x => {sparkSession.read.textFile(x)})

      val wikiPagesWithError = wikiPagesWithErrorRepetitions.reduce(_ union _).dropDuplicates()

      wikiPagesWithError
    }
  }
}