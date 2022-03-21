package edu.neu.coe.csye7200.asstamr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalyzeMovieRatingTest extends AnyFlatSpec with Matchers {

  behavior of "Spark"

  val spark: SparkSession = SparkSession
    .builder()
    .appName("AnalyzeMovieRating")
    .master("local")
    .getOrCreate()
  val resource = "src/main/resources/movie_metadata.csv"

  it should "Open movie_metadata.csv in Spark correctly" in {
    spark.read
      .option("header", "true")
      .csv(resource)
      .count() shouldBe 1609
  }

  it should "Calculate mean and std for rating correctly" in {
    val df:DataFrame = spark.read.option("header", "true").csv(resource)
    val result_df = AnalyzeMovieRating.calcMeanAndStd(df)

    result_df.first().getDouble(0) shouldBe 6.453200 +- 0.000001
    result_df.first().getDouble(1) shouldBe 0.998807 +- 0.000001
  }

}
