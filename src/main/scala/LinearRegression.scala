import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
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

    // TODO Return values of actions are always communicated to the driver program.
    // TODO Distributable actions for dataframes??

    // FEATURE SELECTION ::  Selects for the features we want to train on.
    def parseTweets(filePath: String): DataFrame = {
      val tweetSchema: StructType = ss.read.json(filePath).schema

      val tweetsDataframe = ss.read
        .json(filePath)
        .withColumn("text", functions.from_json($"text", tweetSchema))

      // retweeted status = true added no new tweets.
      val tweets = tweetsDataframe
        .select(
          (($"reply_count" * 0) + 1).alias("z_0"),
          $"quoted_status.user.followers_count",
          $"quoted_status.created_at",
          $"quoted_status.text",
          $"is_quote_status",
          $"quoted_status.retweet_count",
          $"quoted_status.reply_count",
          $"quoted_status.favorite_count")
        .where("is_quote_status = true")

      tweets // returns a dataset
    }

    // FEATURE SCALING
    def scaleFeatures(inputDataset: DataFrame): DataFrame = {

      val intFields = Map[String, Int](
        "followers_count" -> 0,
        "retweet_count" -> 0,
        "reply_count" -> 0,
        "favorite_count" -> 0
      )

      // Lambdas to apply on the dataFrame.
      val fieldSum = (dataset: DataFrame, field: String) => dataset.select(functions.sum($"$field")).first.getLong(0)
      val fieldMean = (sum: Long, count: Long) => (sum / count)
      val fieldDeviations = (dataset: DataFrame, field: String, mean: Long ) => {
        dataset.select(
          $"${field}",
          ( ($"${field}" - mean) * ($"${field}" - mean) ).alias( s"${field}_deviations" ) )
      }
      val totalFieldDeviation = (dataset: DataFrame, field: String) => dataset.select(functions.sum($"${field}_deviations")).first.getLong(0)

      val getStdDevAndMean = (inputDataset: DataFrame, field: String, count: Long) =>  {
        val sum = fieldSum(inputDataset, field)
        val mean = fieldMean(sum, count)
        val deviations = fieldDeviations(inputDataset, field, mean)
        val totalDeviations = totalFieldDeviation(deviations, field)
        val standardDeviation = sqrt(totalFieldDeviation(deviations, field)/(count - 1))

        (mean, standardDeviation)
      }

      val count = inputDataset.count()

      val stdDevMean = intFields.map{ case(field, value) =>
        field -> getStdDevAndMean(inputDataset, field, count)
      }
      val (flCountMean, flCountStdDev) = stdDevMean("followers_count")
      val (rtCountMean, rtCountStdDev) = stdDevMean("retweet_count")
      val (reCountMean, reCountStdDev) = stdDevMean("reply_count")
      val (fvCountMean, fvCountStdDev) = stdDevMean("favorite_count")

      val standardizedDataframe = inputDataset.select($"z_0", $"created_at", $"text",
        (($"followers_count" - flCountMean )/flCountStdDev ).alias("followers_count"),
        (($"retweet_count" - rtCountMean )/rtCountStdDev ).alias("retweet_count"),
        (($"reply_count" - reCountMean )/reCountStdDev ).alias("reply_count"),
        (($"favorite_count" - fvCountMean )/fvCountStdDev ).alias("favorite_count")
      )
      standardizedDataframe
    }

    val tweets = parseTweets("data/tweets").persist() // persist the features we want.
    val scaledDataset = scaleFeatures(tweets).persist() // TODO some features refuse to scale at all. No idea why
    tweets.unpersist() // we no longer need the tweets RDD

    scaledDataset.show()

    System.in.read() // TODO Remove this for cluster test
  }
}
