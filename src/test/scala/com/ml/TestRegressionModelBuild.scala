package com.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.{Assert, Before, Test}

class TestRegressionModelBuild {
  var sparkSession: SparkSession = _
  var basicWithTradeDF: DataFrame = _

  @Before
  def init() = {
    val sparkConf = new SparkConf().setAppName("TestRegressionModelBuild").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession = spark

    import spark.implicits._

    basicWithTradeDF = Seq(
      (417015253, Some("Male"), Some("2.0"), Some(43.0), "0", None, None),
      (611219792, Some("Female"), Some("2.0"), Some(29.0), "0", Some(3), Some(8795.0)),
      (575198932, Some("Female"), Some("1.0"), Some(55.0), "0", Some(2), Some(5159.0)),
      (608552417, None, None, None, "1", None, None),
      (763107752, None, None, Some(20.0), "1", Some(2), Some(6029.0))
    ).toDF("id", "gender", "edu", "age", "is_vip", "trade_cnt", "trade_amt")

  }

  @Test
  def testFeatureBuild() = {
    // 读取数据
    val basicDF = RegressionModelBuild.readData(sparkSession, "/data/df_basic.csv")
    val tradeDF = RegressionModelBuild.readData(sparkSession, "/data/df_trade.csv")

    val basicWithTradeDF = RegressionModelBuild.featureBuild(sparkSession, tradeDF, basicDF)
    basicWithTradeDF.show()
    basicWithTradeDF.printSchema()
  }

  @Test
  def testCalcNullRate() = {
    val spark = sparkSession

    basicWithTradeDF.show()
    val nullRateDF = RegressionModelBuild.calcNullRate(spark, basicWithTradeDF)
    nullRateDF.show()
  }

  @Test
  def testGetModeValue() = {
    val spark = sparkSession
    import spark.implicits._

    val eduModeVal = RegressionModelBuild.getModeValue(spark, basicWithTradeDF, $"edu")
    Assert.assertEquals("2.0", eduModeVal)

    val genderModeVal = RegressionModelBuild.getModeValue(spark, basicWithTradeDF, $"gender")
    Assert.assertEquals("Female", genderModeVal)

    val isVipModeVal = RegressionModelBuild.getModeValue(spark, basicWithTradeDF, $"is_vip")
    Assert.assertEquals("0", isVipModeVal)
  }
}
