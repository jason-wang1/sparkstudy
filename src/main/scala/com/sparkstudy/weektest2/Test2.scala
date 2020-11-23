package com.sparkstudy.weektest2


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Descreption: XXXX<br/>
  * Date: 2019年08月26日
  *
  * @author WangBo
  * @version 1.0
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test2").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    import org.apache.spark.sql.functions._

    val videologs: DataFrame = spark.read.parquet("D://data/1563336707000aj235jk6")


//    videologs.groupBy("ct", "video_type", "video_produced_area", "video_produced_time")
//    .agg(sum("video_duration_play"), sum("ad_duration_play"), countDistinct("user"))

    val videoSchema = StructType(Seq(
      StructField("ct", LongType),
      StructField("video_type", StringType),
      StructField("video_produced_area", StringType),
      StructField("video_produced_time", StringType),
      StructField("video_duration_play", StringType),
      StructField("ad_duration", StringType),
      StructField("user", StringType)
    ))

    val videoEncoder = RowEncoder(videoSchema)

    val splited: Dataset[Row] = videologs.map(line => {
      val ct: Long = line.getAs[Long]("ct")
      val video_type: String = line.getAs[String]("video_type")
      val video_produced_area: String = line.getAs[String]("video_produced_area")
      val video_produced_time: String = line.getAs[String]("video_produced_time")
      val video_duration_play: String = line.getAs[String]("video_duration_play")
      val ad_duration: String = line.getAs[String]("ad_duration")
      val user: String = line.getAs[String]("user")
      Row(ct, video_type, video_produced_area, video_produced_time,
        video_duration_play, ad_duration, user)
    })(videoEncoder)

    val result: DataFrame = splited
      .groupBy($"ct", $"video_type", $"video_produced_area", $"video_produced_time")
      .agg(sum("video_duration_play"),
        sum("ad_duration"),
        countDistinct("user"))

    result.show()

    sc.stop()
    spark.stop()

  }

}





case class videoLogs(video_type: String ,
video_style: String,
video_duration: String,
video_duration_play: String,
ad_type: String,
ad_duration: String,
ad_duration_play: String,
video_tag: String,
video_sections: String,
video_produced_area: String,
video_produced_time: String,
video_director: String,
video_actor: String,
video_name: String,
user: String,
ct: BigInt)
