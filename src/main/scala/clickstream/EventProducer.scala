package clickstream

import java.util.Properties

import clickstream.domain.EventRecord
import com.typesafe.scalalogging.LazyLogging
import config.Settings
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Created by kuzmende on 10/14/17.
  */
object EventProducer extends App with LazyLogging {

  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(
    getClass.getResourceAsStream("/products.csv")).getLines().toArray

  val Referrers = scala.io.Source.fromInputStream(
    getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  val Pages = (0 to wlc.pages).map("Page-" + _)

  val rnd = new Random()

  val topic = wlc.kafkaTopic
  val props = new Properties()

  props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(KEY_SERIALIZER_CLASS_CONFIG, "utils.serde.KryoSerializer")
  props.put(VALUE_SERIALIZER_CLASS_CONFIG, "utils.serde.KryoSerializer")
  props.put(ACKS_CONFIG, "all")
  props.put(CLIENT_ID_CONFIG, "EventProducer")

  val kafkaProducer: Producer[Integer, EventRecord] = new KafkaProducer[Integer, EventRecord](props)
  logger.info(s"${kafkaProducer.partitionsFor(topic)}")

  for (fileCount <- 1 to wlc.numberOfFiles) {

    // introduce some randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis()

      // move all this to a function
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val producerRecord = new ProducerRecord[Integer, EventRecord](topic, fileCount, EventRecord(adjustedTimestamp, referrer, action, prevPage, visitor, page, product))
      kafkaProducer.send(producerRecord)

      if (iteration % incrementTimeEvery == 0) {
        logger.info(s"Sent $iteration messages!")

        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        logger.info(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }

    val sleeping = 2000
    logger.info(s"Sleeping for $sleeping ms")
  }

  kafkaProducer.close()
}