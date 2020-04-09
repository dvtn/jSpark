package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * cogroup: 将两个RDD的Key合并，每个RDD中的Key对应一个value集合
 */
public class cogroupOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("intersection");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("刘备", "武将-关羽"),
                new Tuple2<String, String>("刘备", "武将-张飞"),
                new Tuple2<String, String>("刘备", "武将-赵云"),
                new Tuple2<String, String>("刘备", "武将-马超"),
                new Tuple2<String, String>("刘备", "武将-黄忠"),
                new Tuple2<String, String>("曹操", "武将-张辽"),
                new Tuple2<String, String>("曹操", "武将-典韦"),
                new Tuple2<String, String>("曹操", "武将-夏侯惇"),
                new Tuple2<String, String>("曹操", "武将-许褚"),
                new Tuple2<String, String>("曹操", "武将-徐晃"),
                new Tuple2<String, String>("孙权","武将-周瑜")
        ), 4);

        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("刘备", "谋士-诸葛亮"),
                new Tuple2<String, String>("刘备", "谋士-庞统"),
                new Tuple2<String, String>("曹操", "谋士-郭嘉"),
                new Tuple2<String, String>("曹操", "谋士-荀彧")
        ), 2);


        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD = rdd1.cogroup(rdd2);

        /**
         *
         * cogroup生成的RDD的分区数为：4
         * (曹操,([武将-张辽, 武将-典韦, 武将-夏侯惇, 武将-许褚, 武将-徐晃],[谋士-郭嘉, 谋士-荀彧]))
         * (孙权,([武将-周瑜],[]))
         * (刘备,([武将-关羽, 武将-张飞, 武将-赵云, 武将-马超, 武将-黄忠],[谋士-诸葛亮, 谋士-庞统]))
         */

        System.out.println("cogroup生成的RDD的分区数为："+cogroupRDD.partitions().size());

        cogroupRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });
    }
}
