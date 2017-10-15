package streaming

import clickstream.domain.EventRecord
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import domain.{ActivityByProduct, VisitorsByProduct}
import functions._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.max
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import utils.SparkUtils.{getSparkSession, getStreamingContext}
import utils.serde.KryoDeserializer

import scala.util.Try


/**
  * Created by kuzmende on 10/15/17.
  */
object StreamingJob extends App {

  // setup spark context
  val spark = getSparkSession("clickstream-analytics")
  val batchDuration = Seconds(4)

  import spark.implicits._


  def streamingApp(sc: SparkContext, batchDuration: Duration) = {

    val ssc = new StreamingContext(sc, batchDuration)
    val wlc = Settings.WebLogGen
    val topic = wlc.kafkaTopic

    val kafkaDirectParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "lambda",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),

      "key.deserializer" -> classOf[KryoDeserializer],
      "value.deserializer" -> classOf[KryoDeserializer]
    )

    val hdfsPath = wlc.kafkaOffsets
    var fromOffsets: collection.Map[TopicPartition, Long] = Map()

    Try {
      val hdfsData = spark.read.parquet(hdfsPath)

      fromOffsets = hdfsData
        .groupBy("topic", "kafkaPartition")
        .agg(max("untilOffset").as("untilOffset"))
        .collect().map { row =>
        (new TopicPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")),
          row.getAs[String]("untilOffset").toLong + 1)
      }.toMap
    }

    val kafkaDirectStream =
      KafkaUtils.createDirectStream[Integer, EventRecord](
        ssc,
        PreferConsistent,
        Subscribe[Integer, EventRecord](Set(topic), kafkaDirectParams, fromOffsets)
      )

    val activityStream = kafkaDirectStream.transform(input => {
      functions.rddToRDDActivity(input)
    }).cache()

    // save data to HDFS
    activityStream.foreachRDD { rdd =>
      val activityDF = rdd
        .toDF()
        .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")

      activityDF
        .write
        .partitionBy("topic", "kafkaPartition", "timestamp_hour")
        .mode(SaveMode.Append)
        .parquet(hdfsPath)
    }

    // activity by product
    val activityStateSpec =
      StateSpec
        .function(mapActivityStateFunc)
        .timeout(Minutes(120))

    val statefulActivityByProduct = activityStream.transform(rdd => {
      rdd.toDF()
        .createOrReplaceTempView("activity")

      val activityByProduct = spark.sql(
        """SELECT
              product,
              timestamp_hour,
              sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
              sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
              sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
              from activity
              group by product, timestamp_hour """)

      activityByProduct
        .map { r =>
          ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          )
        }.rdd
    }).mapWithState(activityStateSpec)


    val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()

    activityStateSnapshot
      .reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(30 / 4 * 4),
        filterFunc = (record) => false
      )
      .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
        .toDF().createOrReplaceTempView("ActivityByProduct"))

    // unique visitors by product
    val visitorStateSpec =
      StateSpec
        .function(mapVisitorsStateFunc)
        .timeout(Minutes(120))

    val statefulVisitorsByProduct = activityStream.map(a => {
      val hll = new HyperLogLogMonoid(12)
      ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
    }).mapWithState(visitorStateSpec)

    val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
    visitorStateSnapshot
      .reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(30 / 4 * 4),
        filterFunc = (record) => false
      ) // only save or expose the snapshot every x seconds
      .foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
      .toDF().createOrReplaceTempView("VisitorsByProduct"))

    ssc
  }

  val ssc = getStreamingContext(streamingApp, spark.sparkContext, batchDuration)
  //ssc.remember(Minutes(5))
  ssc.start()
  ssc.awaitTermination()
}

