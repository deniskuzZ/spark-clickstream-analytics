package batch

import config.Settings
import org.apache.spark.sql.SaveMode
import utils.SparkUtils.getSparkSession

/**
  * Created by kuzmende on 10/15/17.
  */
object BatchJob extends App {

  // setup spark context
  val spark = getSparkSession("clickstream-analytics")

  val wlc = Settings.WebLogGen

  // initialize input RDD
  val inputDF = spark.read.parquet(wlc.kafkaOffsets)
    .where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6")

  inputDF.createOrReplaceTempView("activity")

  val visitorsByProduct = spark.sql(
    """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
      |FROM activity GROUP BY product, timestamp_hour
    """.stripMargin)

  val activityByProduct = spark.sql(
    """SELECT
              product,
              timestamp_hour,
              sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
              sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
              sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
              from activity
              group by product, timestamp_hour """).cache()

  activityByProduct
    .write
    .partitionBy("timestamp_hour")
    .mode(SaveMode.Append)
    .parquet(s"${wlc.hdfsPath}/batch")

  visitorsByProduct.rdd.foreach(println)
  activityByProduct.rdd.foreach(println)
}
