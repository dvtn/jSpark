package com.dvtn.core.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 广播变量
 *
 * 1.不能将RDD广播出去，可以将RDD的结果广播出去
 * 2.广播变量只能在Driver端定义，在Executor端使用，不能在Executor端改变。
 * 3.如果不使用广播变量在一个Executor中有多少个task，就有多少变量副本；如果使用广播变量，在每个Executor中只有一份Driver端的变量副本
 *
 */
public class BroadcastFilter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Broadcast");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        /**
         * 下面代码存在的问题：
         * 在Driver端定义变量，并且在Executor端用到了Driver端的变量，如果这个变量的内容非常大，
         * 这样发送task时都要发送这个变量内容到Executor端。
         *
         * 在Driver端可以通过定义广播变量发送到Executor端，这样只需要使用一个广播变量的副本
         */
        List<String> blackList = new ArrayList<>();
        blackList.add("刘备");
        blackList.add("三国演义");
        blackList.add("金庸");

        //定义广播变量
        Broadcast<List<String>> broadcastBlackList = sc.broadcast(blackList);

        JavaRDD<String> lines = sc.textFile("./data/words");

        JavaRDD<String> words = lines.flatMap((line) -> {
            return Arrays.asList(line.split(" ")).iterator();
        });

        JavaRDD<String> filterWords = words.filter((x) -> {
            //在Driver端使用广播变量
            List<String> bl = broadcastBlackList.value();
            return !bl.contains(x);
        });

        JavaRDD<String> distinctRDD = filterWords.distinct();

        distinctRDD.foreach((x)->{
            System.out.println(x);
        });

        sc.stop();
    }
}
