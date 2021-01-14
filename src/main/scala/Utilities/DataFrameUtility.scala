
import java.io._

package Utilities {

  object DataFrameUtility {

    def collectParquetFilesFromFolders(folders: Array[String]): Array[String] = {

      var allParquetFiles: Array[String] = Array[String]()

      folders.foreach(folderName => {

        val folder = new File(folderName)

        val files: Array[String] = folder.listFiles.filter(file => file.isFile && (file.toString.takeRight(15) == ".snappy.parquet")).map(file => file toString)

        allParquetFiles = allParquetFiles ++ files

      })

      allParquetFiles
    }
  }
}