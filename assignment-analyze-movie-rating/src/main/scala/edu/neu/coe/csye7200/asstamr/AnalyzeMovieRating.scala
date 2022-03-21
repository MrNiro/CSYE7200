package edu.neu.coe.csye7200.asstamr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{mean, stddev}


object AnalyzeMovieRating extends App {
  def calcMeanAndStd(df: DataFrame): DataFrame = {
    val colName = "imdb_score"
    df.select(
      mean(df(colName)).alias("mean"),
      stddev(df(colName)).alias("std_dev")
    )
  }

  val spark: SparkSession = SparkSession
    .builder()
    .appName("AnalyzeMovieRating")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.

  val resource = "src/main/resources/movie_metadata.csv"
  val df:DataFrame = spark.read.option("header", "true").csv(resource)

  val result_df:DataFrame = calcMeanAndStd(df)
  result_df.show()
}
