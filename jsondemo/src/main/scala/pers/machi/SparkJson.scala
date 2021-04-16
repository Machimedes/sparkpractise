package pers.machi

import org.apache.spark.sql.SparkSession
import com.google.gson.JsonParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, element_at, from_utc_timestamp, instr, regexp_replace, substring, translate}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object SparkJson {

    val logger = Logger.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        val Array(sinkType) = args

        val spark = SparkSession
            .builder
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._


        val source = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka.bootstrap.servers")
            .option("subscribe", "subscribe")
            .option("startingOffsets", "startingOffsets")
            .option("maxOffsetsPerTrigger", 100)
            .option("failOnDataLoss", "failOnDataLoss")
            .option("groupIdPrefix", "groupIdPrefix")
            .load()


        val transform = source
            .selectExpr("partition", "offset", "CAST(value AS STRING) as value")
            .withColumn("kv",
                extractMessageAsMapUdf($"value")
            )
            .drop("value")


        transform.writeStream
            .format("console")
            .option("truncate", false)
            .start()
            .awaitTermination()

    }


}
