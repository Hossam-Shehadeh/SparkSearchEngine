import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonArray, BsonString}
import org.mongodb.scala.model.Filters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.{Source, StdIn}
import scala.collection.JavaConverters._

object MongoQuery {
  def main(args: Array[String]): Unit = {

    loadEnvVariables()

    val mongoUri = System.getProperty("MONGO_URI")
    val client: MongoClient = MongoClient(mongoUri)
    val database: MongoDatabase = client.getDatabase("sparkdb")
    val collection: MongoCollection[Document] = database.getCollection("dictionary")

    println("Enter your search query:")
    val queryInput = StdIn.readLine()
    val queryWords = queryInput.split("\\s+").map(_.toLowerCase).toSet

    val results = queryWords.map { word =>
      val future = collection.find(equal("word", word)).toFuture()
      val documents = Await.result(future, 10.seconds).flatMap { doc =>
        doc.get("documents").flatMap {
          case array: BsonArray =>
            Some(array.getValues.asScala.map(_.asString().getValue).toSet)
          case _ =>
            None
        }.getOrElse(Set.empty)
      }
      documents
    }

    val commonDocs = results.reduceOption((a, b) => a.intersect(b)).getOrElse(Set.empty)

    if (commonDocs.nonEmpty) {
      println(commonDocs.mkString(", "))
    } else {
      println("No documents found.")
    }

    client.close()

  }
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
