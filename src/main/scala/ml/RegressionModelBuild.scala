package ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * 特征工程+模型训练
  */
object RegressionModelBuild {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RegressionModelBuild").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据
    val basicDF = readData(spark, "/data/df_basic.csv")
    val tradeDF = readData(spark, "/data/df_trade.csv")

    // 1. 特征构造
    val basicWithTradeDF = featureBuild(spark, tradeDF, basicDF).cache()

    // 2. 统计各列的缺失值比例；缺失值填充
    calcNullRate(spark, basicWithTradeDF).show(false)

    val basicFillNullDF = fillNull(spark, basicWithTradeDF).cache()
    basicFillNullDF.show()
    basicFillNullDF.printSchema()

    // 3. 使用回归模型计算trade_cnt(x)对trade_amt(y)的影响
    coffBetweenCntAmt(basicFillNullDF)
  }

  /**
    * 读取数据
    */
  def readData(spark: SparkSession, relativePath: String) = {
    val basicPath = this.getClass.getResource(relativePath).getPath
    spark.read.option("header", "true").csv(basicPath).cache()
  }

  /**
    * 特征构造
    * 在df_basic中，根据df_trade的数据信息，衍生特征“每个客户的交易笔数”并添加至df_basic中， 命名为'trade_cnt'；
    * 在df_basic中，根据df_trade的数据信息，衍生特征“每个客户的交易总金额”并添加至df_basic中， 命名为'trade_amt';
    */
  def featureBuild(spark: SparkSession, tradeDF: DataFrame, basicDF: DataFrame) = {
    import spark.implicits._
    val tradeAggDF = tradeDF.groupBy($"id")
      .agg(count($"amt").alias("trade_cnt"), sum($"amt").alias("trade_amt"))

    basicDF.join(tradeAggDF, Seq("id"), "left")
      .withColumn("age", $"age".cast(DoubleType))
  }

  /**
    * 将null替换为1，否则替换为0，目的是为了统计各列的缺失值比例
    */
  def change(colName: Column) ={
    when(isnan(colName) || isnull(colName), 1).otherwise(0)
  }

  /**
    * 统计各列的缺失值比例
    */
  def calcNullRate(spark: SparkSession, basicWithTradeDF: DataFrame) = {
    import spark.implicits._
    basicWithTradeDF
      .withColumn("gender", change($"gender"))
      .withColumn("edu", change($"edu"))
      .withColumn("age", change($"age"))
      .withColumn("is_vip", change($"is_vip"))
      .withColumn("trade_cnt", change($"trade_cnt"))
      .withColumn("trade_amt", change($"trade_amt"))
      .agg(
        (sum($"gender") / count($"gender")).alias("gender"),
        (sum($"edu") / count($"edu")).alias("edu"),
        (sum($"age") / count($"age")).alias("age"),
        (sum($"is_vip") / count($"is_vip")).alias("is_vip"),
        (sum($"trade_cnt") / count($"trade_cnt")).alias("trade_cnt"),
        (sum($"trade_amt") / count($"trade_amt")).alias("trade_amt")
      )
  }

  /**
    * 计算dataFrame指定列的众数
    */
  def getModeValue(spark: SparkSession, dataFrame: DataFrame, colName: Column): String ={
    dataFrame
      .na.drop()
      .groupBy(colName)
      .count()
      .rdd
      .map { row => (row.getString(0), row.getLong(1)) }
      .reduce{case ((val1, cnt1), (val2, cnt2)) =>
        if (cnt1 > cnt2) (val1, cnt1)
        else (val2, cnt2)
      }
      ._1
  }

  /**
    * 计算dataFrame指定列的中位数
    */
  def getMedianValue(dataFrame: DataFrame, colName: String): Double={
    val quantiles = dataFrame
      .na.drop()
      .stat
      .approxQuantile(colName, Array(0.5), 0.01)
    quantiles(0)
  }

  /**
    * 缺失值填充
    * 对trade_cnt、trade_amt的缺失值填充为0；
    * 对字符型特征的缺失值进行众数填充；
    * 对数值型特征的缺失值进行中位数填充
    */
  def fillNull(spark: SparkSession, dataFrame: DataFrame) = {
    import spark.implicits._

    val genderMode = getModeValue(spark, dataFrame, $"gender")
    val eduModel = getModeValue(spark, dataFrame, $"edu")
    val isVipMode = getModeValue(spark, dataFrame, $"is_vip")
    val ageMedianValue = getMedianValue(dataFrame, "age")

    dataFrame.na
      .fill(Map("gender" -> genderMode, "edu" -> eduModel,
        "age" -> ageMedianValue, "is_vip" -> isVipMode,
        "trade_cnt" -> 0, "trade_amt" -> 0
      ))
  }

  /**
    * 回归模型
    * 请构建回归模型(注明：使用sklearn.linear_model内的函数)，求解trade_cnt(x)对trade_amt(y)的影响,并对输出结果做简要解读；
    *
    * 提示：出于对强解释性及模型精度的考虑，请尽量从特征加工/特征衍生等角度进行预处理，提升模型的 𝑅2
    */
  def coffBetweenCntAmt(basicFillNullDF: DataFrame) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("trade_cnt"))
      .setOutputCol("features")

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.01)
      .setFeaturesCol("features")
      .setLabelCol("trade_amt")

    val pipeline = new Pipeline()
      .setStages(Array(assembler, lr))

    val model: PipelineModel = pipeline.fit(basicFillNullDF)

    val lrModel = model.stages(1).asInstanceOf[LogisticRegressionModel]
    println(s"Coefficients: ${lrModel.coefficientMatrix}  Intercept: ${lrModel.interceptVector}")
  }
}
