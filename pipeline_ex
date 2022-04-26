package com.xpandit.bdu.training.sparkkafka

import java.util.concurrent.TimeUnit
import com.xpandit.bdu.training.commons.JsonSerDes
import com.xpandit.bdu.training.commons.spark.SparkUtils
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

/**
 * Main class for the Spark app.
 */
object SparkKafkaChallengeMain extends App {

  val SparkAppName = "Spark-Kafka integration challenge"
  println(s"Print by Spark's driver of: $SparkAppName")

  val spark = SparkUtils.getOrCreateSparkSession(SparkAppName, "local[*]", hiveIntegrationEnabled = false)
  import spark.implicits._

  val kafkaDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Configs.kafkaBrokers)
    .option("subscribe", Configs.kafkaInputTopic)
    .option("startingOffsets", "earliest")    // Start in the begin of the topic partitions.
    .load()

  kafkaDF.printSchema()

  val transformedDataDSString = kafkaDF.selectExpr("CAST (value as STRING)")

  val schema = new StructType()
    .add("name", dataType = "string")
    .add("value", dataType = "string")
    .add("double_value", dataType = "string")

  val transformedDataDS= transformedDataDSString.select(from_json(col("value"), schema).as("data"))
    .select("data.*")

/*  transformedDataDS.writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime(TimeUnit.SECONDS.toMillis(2)))
    .start()
    .awaitTermination()*/

  transformedDataDS.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", Configs.kafkaBrokers)
    .option("topic", Configs.kafkaOutputTopic)
    .option("checkpointLocation", "/tmp")
    .queryName("Data Transformation Streaming Query")
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime(TimeUnit.SECONDS.toMillis(2)))
    .start()
    .awaitTermination()

}
