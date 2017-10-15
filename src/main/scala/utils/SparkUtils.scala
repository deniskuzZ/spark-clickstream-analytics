package utils

import config.Settings
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by kuzmende on 10/15/17.
  */
object SparkUtils {

  def getSparkSession(appName: String) = {
    var checkpointDirectory = Settings.WebLogGen.hdfsPath

    val spark = SparkSession.builder
      .master("local[*]")
      .appName(appName)
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir(checkpointDirectory)

    spark
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)

    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
