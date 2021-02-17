import Utilities.DataFrameUtility
import org.apache.spark.sql.{SQLContext, SparkSession}

object showData extends App {
  override def main(args: Array[String]) {

    val sparkSession = SparkSession.builder().master("local[4]").appName("showData").getOrCreate()
    //val sparkSession = SparkSession.builder().appName("showData").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()
/*
    val path = args(0)
    val dataFrames = args.drop(1) map (tempFile => sparkSession.read.parquet(path+tempFile))

 */

    //val inputFile = sparkContext.textFile("file.txt").collect

    /*val dataFrames = inputFile.map(line => {
        println(path + line)
        sparkSession.read.parquet(path + line)
      }
    )*/

    //merge dei parquet in un dataFrame unico

    /*
    val dataFrameTotal = dataFrames.reduce(_ union _)

    dataFrameTotal.coalesce(1).write.parquet("s3n://wtt-s3-1/provaOutput")

     */

    //sparkContext.parallelize(Array[String]("pippo", "pluto", "paperino")).coalesce(1).saveAsTextFile("s3n://wtt-s3-1/exp/fileSingolo.txt")

    /*
    DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati"), "en", sparkSession).coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\nuovaCartella\\en")
    DataFrameUtility.dataFrameFromFoldersRecursively(Array("C:\\Users\\nik_9\\Desktop\\prova\\outputProcessati"), "it", sparkSession).coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\nuovaCartella\\it")
    */


    //res.show(false)

    //println(res.count())

    //res.coalesce(1).write.parquet("C:\\Users\\nik_9\\Desktop\\prova\\nuovaCartella\\en")

    val sqlContext = sparkSession.sqlContext

    val accessKeyId = "ASIAWEHWHRM2ID6ERWPJ"
    val secretAccessKey = "+k5EA0lHBDY/bO0s1NjMzqpWarts0U1WY/G3P7nt"

    sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
    sparkContext.hadoopConfiguration.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    val df = sqlContext.read
      //.format("com.databricks.spark.csv")
      //.option("header", "true")
      //.option("inferSchema", "true")
      .load("s3n://wtt-s3-1/indici/file53.txt")

    //val in = sparkSession.read.load("s3n://wtt-s3-1/indici/file53.txt")

    df.foreach(x => println(x))

    val endTime = System.currentTimeMillis()

    val minutes = (endTime - startTime) / 60000
    val seconds = ((endTime - startTime) / 1000) % 60

    println("Time: " + minutes + " minutes " + seconds + " seconds")

    sparkSession.stop()
  }
}
