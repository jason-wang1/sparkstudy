package com.sparkstudy.day07;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Descreption: XXXX<br/>
 * Date: 2019年07月02日
 *
 * @author WangBo
 * @version 1.0
 */
public class LambdaJavaWC {
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("lambdajavawc").setMaster("local");
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(new SparkContext(conf));

        final JavaRDD<String> lines = jsc.textFile("hdfs://node01:9000/files");
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        final JavaPairRDD<String, Integer> tuples = words.mapToPair(word -> new Tuple2<>(word, 1));

    }
}
