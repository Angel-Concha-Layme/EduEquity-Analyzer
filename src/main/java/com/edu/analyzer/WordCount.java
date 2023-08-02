package com.edu.analyzer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[1]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> input = context.textFile("path");
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                });

        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            public Integer call(Integer a, Integer b) {
                                return a + b;
                            }
                        });

        List<Tuple2<String, Integer>> output = wordCounts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        context.stop();
        context.close();
    }
}