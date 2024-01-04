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
//    conf.setMaster("local[2]") // TODO Remove this for Isabelle cluster test
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
      println("Parsing tweets")
      val tweetSchema: StructType = ss.read.json(filePath).schema

      val tweetsDataframe = ss.read
        .json(filePath)
        .withColumn("text", functions.from_json($"text", tweetSchema))
        .persist()

      // retweeted status = true added no new tweets.
      val tweets = tweetsDataframe
        .select(
          (($"quoted_status.reply_count" * 0) + 1).alias("z_0"),
          $"quoted_status.user.followers_count",
          $"quoted_status.created_at",
          $"quoted_status.text",
          $"is_quote_status",
          $"quoted_status.retweet_count",
          $"quoted_status.reply_count",
          $"quoted_status.favorite_count")
        .where("is_quote_status = true")

      tweetsDataframe.unpersist()
//      tweets.repartition(8, $"favorite_count") // TODO remove for Isabelle
      tweets.repartition(80, $"favorite_count") // TODO uncomment for Isabelle
//      tweets // returns a dataframe
    }

    // FEATURE SCALING
    def scaleFeatures(inputDataset: DataFrame): DataFrame = {
      println("Scaling features")
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
      val (flCountMean, flCountStdDev) = stdDevMean("followers_count") //followers mean && stdDev
      val (rtCountMean, rtCountStdDev) = stdDevMean("retweet_count") //retweetCount mean && stdDev
      val (reCountMean, reCountStdDev) = stdDevMean("reply_count") //replyCount mean && stdDev
      val (fvCountMean, fvCountStdDev) = stdDevMean("favorite_count") //favorite mean && stdDev

      val standardizedDataframe = inputDataset.select($"z_0", $"created_at", $"text",
        (($"followers_count" - flCountMean )/flCountStdDev ).alias("followers_count"),
        (($"retweet_count" - rtCountMean )/rtCountStdDev ).alias("retweet_count"),
        (($"reply_count" - reCountMean )/reCountStdDev ).alias("reply_count"),
        (($"favorite_count" - fvCountMean )/fvCountStdDev ).alias("favorite_count")
      )
        .where("followers_count > -1 and followers_count < 1")
        .where("retweet_count > -1 and retweet_count < 1")
        .where("reply_count > -1 and reply_count < 1")
        .where("favorite_count > -1 and favorite_count < 1")
      // we apply a post filter because some values still do not get standardized properly and we end up with
      // values > 1 and < -1

      standardizedDataframe
    }

    // multivariate linear regression
    def errorFunction(inputDataset: DataFrame, param_followers_count: Double, param_reply_count: Double, param_retweet_count: Double, count: Long): Double = {
      // calculate the mean squared error
      val dataFrameWithCost = inputDataset.withColumn("cost",
        ((
          ($"followers_count" * param_followers_count) +
          ($"reply_count" * param_reply_count) +
          ($"retweet_count" * param_retweet_count) +
          ($"favorite_count" * -1 )
          ) * (
          ($"followers_count" * param_followers_count) +
            ($"reply_count" * param_reply_count) +
            ($"retweet_count" * param_retweet_count) +
            ($"favorite_count" * -1)
          ) )
      )
      val sumOfCosts = dataFrameWithCost.select( functions.sum($"cost")).first.getDouble(0)
      return (sumOfCosts ) / (2*count)
    }

    def accuracyTest(inputDataset: DataFrame, param_followers_count: Double, param_reply_count: Double, param_retweet_count: Double): Double = {
      println("Testing the models accuracy")
      val count = inputDataset.count()
      val calculatedY = inputDataset.withColumn("predicted_likes", (
        ($"followers_count" * param_followers_count) +
        ($"reply_count" * param_reply_count) +
        ($"retweet_count" * param_retweet_count)
      ))

      val mse = calculatedY.withColumn( "mean_squared_error", (
        ($"predicted_likes" - $"favorite_count") * ($"predicted_likes" - $"favorite_count")
      )).select( functions.sum( $"mean_squared_error") / count  ).first.getDouble(0)

      mse
    }

    def multivariateLinearRegression(inputDataset: DataFrame, alpha: Double) = {
      println("Performing gradient descent: ")
      val fieldSum = (dataset: DataFrame, field: String) => dataset.select(functions.sum($"$field")).first.getDouble(0)
      val fieldMean = (sum: Double, count: Long) => (sum / count)
      val count = inputDataset.count()

      var delta = 0.00

      var param_followers_count = fieldMean(fieldSum(inputDataset, "followers_count"), count)
      var param_reply_count = fieldMean(fieldSum(inputDataset, "reply_count"), count)
      var param_retweet_count = fieldMean(fieldSum(inputDataset, "retweet_count"), count)

      var error = errorFunction(inputDataset, param_followers_count, param_reply_count, param_retweet_count, count)
      val features = Array("followers_count", "reply_count", "retweet_count")
      // favorite_count = h0 + (param_followers_count * followers_count) + (param_reply_count * reply_count )
      // + (param_retweet_count * retweet_count )

      do {
        param_followers_count = param_followers_count - inputDataset.withColumn("followers_param",
          (
            (($"followers_count" * param_followers_count) + //h_0(X^i)
              ($"reply_count" * param_reply_count) + //h_0(X^i)
              ($"retweet_count" * param_retweet_count) + //h_0(X^i)
              ($"favorite_count" * -1)) * // y^(i)
              $"followers_count" // x_j^(i)
            )
        ).select(functions.sum($"followers_param")).first.getDouble(0) * (alpha / count)

        param_reply_count = param_reply_count - inputDataset.withColumn("reply_param",
          (
            (($"followers_count" * param_followers_count) +
              ($"reply_count" * param_reply_count) +
              ($"retweet_count" * param_retweet_count) +
              ($"favorite_count" * -1)) * $"reply_count"
            )
        ).select(functions.sum($"reply_param")).first.getDouble(0) * (alpha / count)

        param_retweet_count = param_retweet_count - inputDataset.withColumn("retweet_param",
          (
            (($"followers_count" * param_followers_count) +
              ($"reply_count" * param_reply_count) +
              ($"retweet_count" * param_retweet_count) +
              ($"favorite_count" * -1)) * $"retweet_count"
            )
        ).select(functions.sum($"retweet_param")).first.getDouble(0) * (alpha / count)

        var j0: Double = errorFunction(inputDataset, param_followers_count, param_reply_count, param_retweet_count,  count)
        delta = error - j0
        error = j0
      } while (delta > 0.00007)
      Array(param_followers_count, param_retweet_count, param_reply_count)
    }

//    val tweets = parseTweets("data/tweets").persist() // persist the features we want. TODO comment out for Isabelle
        val tweets = parseTweets("/data/twitter").persist() // persist the features we want. TODO Uncomment for Isabelle

    val scaledDataset = scaleFeatures(tweets).persist()
    tweets.unpersist() // we no longer need the tweets RDD
    val Array(trainingDataset, testingDataset) = scaledDataset.randomSplit(Array(0.5, 0.5), seed = 42)
    val Array(param_followers_count: Double, param_retweet_count: Double, param_reply_count: Double) = multivariateLinearRegression(trainingDataset, 0.001)
    val accuracy: Double = accuracyTest(trainingDataset, param_followers_count, param_retweet_count, param_reply_count)
    println("Root Mean Squared Error Post Training: ", accuracy)

//     System.in.read() // TODO Remove this for cluster test
  }
}
