import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import scala.io.Source

object tweetStream {

  def main(args: Array[String]): Unit = {
    val accesskeys = authenticate()
    startStream(accesskeys)
  }


    def authenticate(): List[String] = {

      val lines = Source.fromFile("/home/amit/IdeaProjects/logextraction/src/main/scala/Creds").getLines.toList
      lines


    }

    def startStream(accesskeys:List[String]): Unit = {

      val config = new SparkConf().setMaster("local[4]").setAppName("Tweetstreaming")
      val ssc = new StreamingContext(config, Seconds(5))
      val cb = new ConfigurationBuilder



      cb.setDebugEnabled(true).setOAuthConsumerKey(accesskeys(0).toString)
        .setOAuthConsumerSecret(accesskeys(1).toString)
        .setOAuthAccessToken(accesskeys(2).toString)
        .setOAuthAccessTokenSecret(accesskeys(3).toString)

      val auth = Some(new OAuthAuthorization(cb.build))
      val twitterStream = TwitterUtils.createStream(ssc, auth)


      val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
      val desitweets = twitterStream.filter(x => x.getLang() =="en").filter(x => x.getUser().getLocation() == "India")


      desitweets.print()

      ssc.start()

      ssc.awaitTermination()

    }

}