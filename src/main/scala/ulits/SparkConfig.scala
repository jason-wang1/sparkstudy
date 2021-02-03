package ulits

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class SparkConfig(appName: String) {
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var sparkContext: SparkContext = _


  def getSparkConf: SparkConf = {
    if(sparkConf == null) {
      sparkConf = new SparkConf().setAppName(appName)
      sparkConf.setMaster("local[*]")
    }
    sparkConf
  }


  def getSparkSession: SparkSession = {
    if(sparkSession == null)
      sparkSession = SparkSession.builder().enableHiveSupport().config(getSparkConf).getOrCreate()
    sparkSession
  }

  def getSparkContext: SparkContext = {
    if (sparkContext == null) {
      sparkContext = getSparkSession.sparkContext
    }
    sparkContext
  }
}
