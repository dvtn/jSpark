package com.dvtn.core.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * join:
 *      public <W> JavaPairRDD<K,scala.Tuple2<V,W>> join(JavaPairRDD<K,W> other,int numPartitions)
 *      按照两个RDD的Key关联，其中，K为联接的键，V为左边RDD中K所对应的值，W为右边RDD中K所对应的值,(V,W)形成一个二元组
 *
 *
 *  注意：
 *  1.必须作用于K,V格式的RDD上
 *  2.join后生成的RDD与join前两个RDD中分区数多的保持一致
 *
 */

public class joinOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("join");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("风清扬", 60),
                new Tuple2<String, Integer>("张三丰", 100),
                new Tuple2<String, Integer>("独孤求败", 40),
                new Tuple2<String, Integer>("东方不败", 38),
                new Tuple2<String, Integer>("任我行", 58),
                new Tuple2<String, Integer>("王重阳", 62),
                new Tuple2<String, Integer>("洪七公", 55),
                new Tuple2<String, Integer>("老顽童", 50),
                new Tuple2<String, Integer>("黄药师", 56),
                new Tuple2<String, Integer>("欧阳锋", 57),
                new Tuple2<String, Integer>("一灯大师", 61),
                new Tuple2<String, Integer>("梅超风", 42)
        ), 4);

        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("风清扬", "剑宗"),
                new Tuple2<String, String>("张三丰", "太极拳"),
                new Tuple2<String, String>("独孤求败", "剑圣"),
                new Tuple2<String, String>("东方不败", "葵花宝典")//,
                //new Tuple2<String, String>("任我行", "乾坤大挪移"),
                //new Tuple2<String, String>("王重阳", "九阳神功"),
                //new Tuple2<String, String>("洪七公", "响龙十八掌"),
                //new Tuple2<String, String>("老顽童", "九阴真经"),
                //new Tuple2<String, String>("黄药师", "弹指神功"),
                //new Tuple2<String, String>("欧阳锋", "蛤蟆功"),
                //new Tuple2<String, String>("一灯大师", "六脉神剑"),
                //new Tuple2<String, String>("梅超风", "九阴白骨爪")
        ), 2);

        /**
         * 第一个String为两个RDD相匹配的键;
         * Integer为第一个RDD中该键对应的值;
         * 每二个String为第二个RDD中该键对应的值;
         */
        JavaPairRDD<String, Tuple2<Integer, String>> joinRDD = rdd1.join(rdd2);

        /**
         * 分区数的变化
         */
        System.out.println("rdd1和rdd2相join后生成的RDD的分区数为："+joinRDD.partitions().size());

        joinRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, String>> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        sc.stop();
    }
}
