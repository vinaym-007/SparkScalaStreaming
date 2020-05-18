package vinay.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/** Listens to a stream of Tweets and will keep track of the most popular tweets in the given timeframe
 */
object PopularHashtags {
  
    /** To ensure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twiter.txt in the main workspace directory and make sure to enter the keys in the followin format
  1. consumerKey 3#######################c
  2. consumerSecret A################################################T
  3. accesstoken 1################################################l
  4. accessTokenSecret P###########################################U
  */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  /** Main function
      The place for Action!!!
   */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Setting up a Spark streaming context named "PopularHashtags" that runs locally using
    // all of the CPU cores with one-second batches of data as input data stream
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Getting rid of log spam (should be called after the context is set up)
    setupLogging()

    // Creating DStream from Twitter using streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Extracting the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // Eliminating anything that's not a hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Mapping each hashtag to a key/value pair of (hashtag, 1) so that we can count them by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    // Counting the popular #value over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(60), Seconds(1))
   
    // Sorting the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Printing the top 10
    sortedResults.print
    
    // Setting up a checkpoint directory
    // and starting the process!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
