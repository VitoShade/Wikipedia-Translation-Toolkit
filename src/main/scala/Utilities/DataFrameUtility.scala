
import java.io._
import org.apache.spark.SparkContext

package Utilities {

  import org.apache.spark.sql.{DataFrame, SparkSession}

  import scala.collection.mutable

  object DataFrameUtility {

    def writeFileID(filePath: String, listID: Vector[String], sparkContext: SparkContext): Unit = {
      sparkContext.parallelize(listID).coalesce(1).saveAsTextFile(filePath)
    }

    def writeFileErrors(filePath: String, listErrors: Vector[(String, Vector[(Int, String)])]): Unit = {
      val file = new File(filePath)
      if(!file.exists) file.createNewFile()
      val bw = new BufferedWriter(new FileWriter(file, true))
      listErrors.foreach( ErrorTuple => bw.write(ErrorTuple._1 + ": " + ErrorTuple._2.map(tuple => "(" + tuple._1 + " , " + tuple._2  + ")" + "\t") + "\n"))
      bw.close()
    }

    def dataFrameFromFoldersRecursively(folders: Array[String], subFolder: String, sparkSession: SparkSession): DataFrame = {

      var allParquetFiles = Array[String]()

      var queue = new mutable.Queue[String]()

      queue ++= folders

      while(queue.nonEmpty) {

        val folder = new File(queue.dequeue())

        if(folder.toString.takeRight(subFolder.length) == subFolder) {

          val files = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file.toString)

          allParquetFiles = allParquetFiles ++ files
        }

        val recursiveFolders = folder.listFiles.filter(file => file.isDirectory).map(file => file.toString)

        queue ++= recursiveFolders
      }

      val dataFrameFiles = allParquetFiles map (tempFile => sparkSession.read.parquet(tempFile))

      //merge dei parquet in un dataFrame unico
      dataFrameFiles.reduce(_ union _)
    }
  }
}