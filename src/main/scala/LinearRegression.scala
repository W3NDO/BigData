import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

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

    // Selects for the features we want to train on.
    def parseTweets(filePath: String): DataFrame = {
      val tweetSchema: StructType = ss.read.json(filePath).schema

      val tweetsDataframe = ss.read
        .json(filePath)
        .withColumn("text", functions.from_json($"text", tweetSchema))
        .persist()

      val tweets = tweetsDataframe
        .select($"quoted_status.user.followers_count", $"quoted_status.created_at", $"quoted_status.text", $"is_quote_status", $"quoted_status.retweet_count", $"quoted_status.reply_count", $"quoted_status.favorite_count")
        .where("is_quote_status = true")
        .where("retweeted = true")

      tweets
    }

    val tweets = parseTweets("data/tweets")
    tweets.show()

    System.in.read() // TODO Remove this for cluster test
  }
}
