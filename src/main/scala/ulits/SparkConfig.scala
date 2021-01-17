package ulits

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkConfig(appName: String) {
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _


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
}
