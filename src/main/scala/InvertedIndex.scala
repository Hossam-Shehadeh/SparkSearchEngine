import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.io.Source

object InvertedIndex {

  def main(args: Array[String]): Unit = {

    // Use function for load environment variables from .env file
    loadEnvVariables()

    // Get MongoDB URI from sys.env
    val mongoUri = System.getProperty("MONGO_URI")
    if (mongoUri == null) {
      throw new RuntimeException("MONGO_URI environment variable not set.")
    }

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("InvertedIndex")
      .master("local[*]")
      .config("spark.mongodb.read.connection.uri", mongoUri)
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Path to the documents folder
    val documentsPath = "documents/*"

    // Read files into an RDD
    val filesRDD: RDD[(String, String)] = spark.sparkContext.wholeTextFiles(documentsPath)

    // Tokenize and create pairs
    val wordDocPairs: RDD[(String, String)] = filesRDD.flatMap { case (filePath, content) =>
      val docName = filePath.split("/").last.replaceAll("\\.txt$", "")
      content.split("\\s+").map(word => (word.toLowerCase, docName))
    }

    // Create the inverted index
    val invertedIndex: RDD[(String, (Int, List[String]))] = wordDocPairs
      .groupByKey()
      .map { case (word, docList) =>
        val docs = docList.toList.distinct.sorted
        val totalCount = docList.size
        (word, (totalCount, docs))
      }
      .sortByKey() // Sort by word

    // Prepare data for saving to a local file
    val outputRDD: RDD[String] = invertedIndex.map { case (word, (count, docs)) =>
      s"$word,$count,${docs.mkString(",")}"
    }

    // Save the output to a local file
    val outputPath = "./output/wholeInvertedIndex"
    outputRDD.coalesce(1).saveAsTextFile(outputPath)

    import spark.implicits._

    // Save to MongoDB
    val invertedIndexDF = invertedIndex.map { case (word, (count, docs)) =>
      (word, count, docs)
    }.toDF("word", "count", "documents")

    invertedIndexDF.write
      .format("mongodb")
      .mode("overwrite")
      .option("collection", "dictionary")
      .save()

  }

  // Get environment variables from a .env file
  def loadEnvVariables(): Unit = {
    val source = Source.fromFile(".env")
    for (line <- source.getLines()) {
      if (!line.trim.startsWith("#") && line.contains("=")) {
        val Array(key, value) = line.split("=", 2).map(_.trim)
        System.setProperty(key, value)
      }
    }
    source.close()
  }
}
