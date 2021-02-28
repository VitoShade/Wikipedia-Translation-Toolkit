import java.io._
import org.apache.spark.SparkContext

package Utilities {

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
  }
}