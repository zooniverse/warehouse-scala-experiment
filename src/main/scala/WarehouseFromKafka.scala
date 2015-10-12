import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import com.databricks.spark.avro._
import org.apache.spark.streaming.kafka.{KafkaUtils}
import spray.json._
import ClassificationJsonProtocol._

object WarehouseFromKafka {

  def main(args: Array[String]) {
    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val classifications = messages.map(_._2.parseJson.convertTo[Classification])

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
