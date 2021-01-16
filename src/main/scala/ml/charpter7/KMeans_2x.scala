package ml

import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.sql.SparkSession

object KMeans_2x {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KMeans_2x")
      .getOrCreate()

    // 读取样本
    val dataset = spark.read.format("libsvm").load("hdfs://1.1.1.1:9000/sample_kmeans_data.txt")
    dataset.show()

    // 训练 a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // 模型指标计算.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 结果显示.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    // 模型保存与加载
    model.save("hdfs://10.49.136.150:9000/user/clustering/kmmodel")
    val load_treeModel = KMeansModel.load("hdfs://1.1.1.1:9000/clustering/kmmodel")
    spark.stop()
  }

}