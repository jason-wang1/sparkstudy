package com.qf.day07;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaSparkWordCount {
    public static void main(String[] args) {
        // 初始化环境
        // 初始化配置文件类
        SparkConf conf = new SparkConf();
        conf.setAppName("javasparkwc");
        conf.setMaster("local[*]");
        // 初始化上下文
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 获取数据
        JavaRDD<String> lines = jsc.textFile("hdfs://node01:9000/files");

        // 切分
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 将单词生成一个个元组
        JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 开始聚合
        JavaPairRDD<String, Integer> sumed = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // Spark Java API里面没有提供sortBy算子，想要按照value进行排序，
        // 需要将数据进行反转
        JavaPairRDD<Integer, String> swaped = sumed.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
//                return new Tuple2<>(tup._2, tup._1);
                return tup.swap();
            }
        });

        // 降序排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        // 反转回来
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });

        System.out.println(res.collect());

        jsc.stop();
    }
}
