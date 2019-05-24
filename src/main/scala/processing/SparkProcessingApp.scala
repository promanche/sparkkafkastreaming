package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, struct, to_json, udf, window}
import org.apache.spark.sql.types.{DataTypes, StructType}

object SparkProcessingApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("simple-streaming")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logs")
      .load()

    val logsJsonDf = inputDf.selectExpr("CAST(value AS STRING) as value")

    val ratesDf = windowedGroupByHostAndLevel(logsJsonDf)

    val allLevelsQuery = ratesDf.select(to_json(struct("*")).alias("value"))
      .writeStream
      .format("kafka")
      .option("format", "append")
      .option("checkpointLocation", "/tmp/cp1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "log_rates")
      .start()

    val errorLogDf = errorRates(ratesDf)

    val errorsQuery = errorLogDf.select(to_json(struct("*")).alias("value"))
      .writeStream
      .format("kafka")
      .option("format", "append")
      .option("checkpointLocation", "/tmp/cp2")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "error_rates")
      .start()

    allLevelsQuery.awaitTermination()
    errorsQuery.awaitTermination()
  }

  def windowedGroupByHostAndLevel(df: DataFrame) = {
    val schema = new StructType()
      .add("timestamp", DataTypes.TimestampType)
      .add("host", DataTypes.StringType)
      .add("level", DataTypes.StringType)
      .add("text", DataTypes.StringType)

    val ratesDf = df.select(from_json(col("value"), schema = schema).as("log"))
      .select("log.*")
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        window(col("timestamp"), "1 minute", "1 minute"),
        col("host"), col("level"))
      .count()
      .select(
        col("window.start").as("ts"),
        col("host"),
        col("level"),
        col("count")
      )
    ratesDf
  }

  def errorRates(df: DataFrame) = {
    val coder: (Int => Float) = (v: Int) => v.toFloat / 60
    val sqlfunc = udf(coder)

    val logNestedDf = df
      .filter(col("level") === "ERROR")
      .filter("count > 60")
      .withColumn("count", sqlfunc(col("count")))
      .select(
        col("ts"),
        col("host"),
        col("count").as("error_rate")
      )
    logNestedDf
  }
}
