import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.math._

object LinearRegression {
  def main(args: Array[String]): Unit = {
    println("In The beginning there was a linear regression algorithm")

    //spark context config
    val conf = new SparkConf()
    conf.setAppName("PN000006")
    conf.setMaster("local[2]") // TODO Remove this for cluster test
    val sc = new SparkContext(conf)

    // spark session config
    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import ss.implicits._

    // We filter the tweets to find these specific features of a quoted tweet
    // All the tweets we train on are quoted tweets because they have like count information.
    /*
      created_at => date the tweet was created
      user.followers_count => how many followers the person who made the tweet has
      is_quote_status => These are the tweets that may have likes on them.
      retweeted_status => Tweets that have been retweeted
      retweet_count => number of rts the tweet has
      reply count => number of replies the tweet has
      favorite_count => number of likes the tweet has?
     */

    // FEATURE SELECTION ::  Selects for the features we want to train on.
    def parseTweets(filePath: String): DataFrame = {
      val tweetSchema: StructType = ss.read.json(filePath).schema

      val tweetsDataframe = ss.read
        .json(filePath)
        .withColumn("text", functions.from_json($"text", tweetSchema))
        // .persist()

      // retweeted status = true added no new tweets.
      val tweets = tweetsDataframe
        .select($"quoted_status.user.followers_count", $"quoted_status.created_at", $"quoted_status.text", $"is_quote_status", $"quoted_status.retweet_count", $"quoted_status.reply_count", $"quoted_status.favorite_count")
        .where("is_quote_status = true")

      tweets // returns a dataset
    }

    val tweets = parseTweets("data/tweets").persist() // persist the stored projection
//     tweets.select($"followers_count").show()

    // FEATURE SCALING

    def scaleFeatures(inputDataset: DataFrame): DataFrame = {
      // formula for integers zi = (xi - mean)/stdDev
      // TODO find formula for datetime
      // TODO find formula for text(TF-IDF????)
      val intFields = ("followers_count", "retweet_count", "reply_count", "favorite_count")
      val allFields = ("created_at", "text", "followers_count", "retweet_count", "reply_count", "favorite_count")
      val getDeviation = (value: Long, mean: Long) => ((value - mean) * (value - mean))

      val fieldSum = (dataset: DataFrame, field: String) => dataset.select(functions.sum($"$field"))
      val fieldCount = (dataset: DataFrame, field: String) => dataset.select($"$field").where(s"${field} > 0")
      val fieldMean = (sum: Long, count: Long) => sum/count
      val fieldDeviations = (dataset: DataFrame, field: String, mean: Long ) => {
        dataset.select( $"${field}", ( ($"${field}" - mean) * ($"${field}" - mean) ).alias( s"${field}_deviations" ) )
      }
      val totalFieldDeviation = (dataset: DataFrame, field: String) => dataset.select(functions.sum($"${field}_deviations"))

      // TODO build a function that returns field_deviation columns for all numeric fields then map it to return the z-score
      // Read on how the functions can be calculated on the worker nodes vs the co-ordinator node.

      val thisFieldCount = fieldCount(inputDataset, "followers_count").first.getLong(0)
      val followersSum = fieldSum(inputDataset, "followers_count").first.getLong(0)
      val mean = fieldMean(followersSum, thisFieldCount)
      val deviations =  fieldDeviations(inputDataset, "followers_count", mean)
      val standardDev = sqrt(fieldMean(totalFieldDeviation(deviations, "followers_count").first.getLong(0), thisFieldCount ))
      println(">> Field Variance:: " + standardDev )
      deviations // TODO change this
    }

    scaleFeatures(tweets).show()

    System.in.read() // TODO Remove this for cluster test
  }
}
