package com.qf.day07;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaJavaWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lambdajavawc").setMaster("local");
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(new SparkContext(conf));

        JavaRDD<String> lines = jsc.textFile("hdfs://node01:9000/files");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> tuples = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> sumed = tuples.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<Integer, String> swaped = sumed.mapToPair(tup -> tup.swap());
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> res = sorted.mapToPair(tup -> tup.swap());

        System.out.println(res.collect());

        jsc.stop();
    }
}
